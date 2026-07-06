"""Generate Markdown documentation for the Dataspot -> I14Y mappings.

Two sections are produced into ``docs/mappings.md``:

1. **Field mappings** - which I14Y model field each Dataspot source field
   feeds, extracted from the transform functions in ``transform.py`` via
   ``ast``. Local variables inside a transform are resolved so the source
   field is surfaced even when it is computed into a local first.
2. **Vocabulary mappings** - the controlled-vocabulary code tables in
   ``mappings.py``. Their human-readable labels live only in trailing
   comments (``"001": "117",  # Bevoelkerung -> Einwohner``), so we parse
   the *source* (data via ``ast``, label comments via ``tokenize``) rather
   than importing the dicts, which would discard the comments.

Usage::

    uv run python scripts/gen_mapping_docs.py            # write docs/mappings.md
    uv run python scripts/gen_mapping_docs.py --check     # exit 1 if out of date
"""

from __future__ import annotations

import argparse
import ast
import sys
import tokenize
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
_DATASPOT = (
    REPO_ROOT / "src" / "metadataswiss_connector" / "sources" / "dataspot"
)
MAPPINGS_PY = _DATASPOT / "mappings.py"
TRANSFORM_PY = _DATASPOT / "transform.py"
OUTPUT_MD = REPO_ROOT / "docs" / "mappings.md"


# --------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------
def _add(seq: list, item) -> None:
    """Append ``item`` to ``seq`` unless already present (order-preserving)."""
    if item not in seq:
        seq.append(item)


def _esc(text: str) -> str:
    """Escape the Markdown table-cell delimiter."""
    return text.replace("|", "\\|")


# --------------------------------------------------------------------------
# Field mappings (transform.py)
# --------------------------------------------------------------------------
# Pydantic model / enum constructors that are structural wrappers, not
# transformations - excluded from the "Transform" column.
_MODEL_NAMES = {
    "CodeInputModel",
    "CodeListConceptInput",
    "CodeListEntrySortProperty",
    "CodeListEntryValueType",
    "DataServiceInputModel",
    "DcatDatasetInputModel",
    "DcatDistributionInputModel",
    "EmailInputModel",
    "IdentifierInputModel",
    "IdModel",
    "MultiLanguageModel",
    "ResourceModel",
    "VCardModel",
}
# Builtins that carry no mapping meaning.
_BUILTIN_NOISE = {
    "sorted", "str", "len", "max", "min", "list", "set", "dict",
    "float", "int", "tuple", "bool", "super", "any", "all", "enumerate",
}
_EXCLUDE_CALLS = _MODEL_NAMES | _BUILTIN_NOISE
# Parameters that are plumbing, not a documentable data source.
_STRUCTURAL_PARAMS = {"record", "children", "raw", "lookups"}


@dataclass(frozen=True)
class FieldMapping:
    """One transform function to document as a field-mapping table.

    ``model`` is the Pydantic constructor whose keyword arguments are the
    I14Y target fields. ``roots`` names the parameters that hold the source
    record/row (``children`` is always treated as the 1:n child-table root).
    ``note`` is curated context the AST cannot derive (e.g. sidecar
    payloads emitted outside the model constructor).
    """

    func: str
    model: str
    title: str
    roots: tuple[str, ...] = ("record", "children")
    note: str = ""


FIELD_MAPPINGS: list[FieldMapping] = [
    FieldMapping(
        "transform_to_dataset",
        "DcatDatasetInputModel",
        "Dataset - Dataspot data product to I14Y dataset",
        note=(
            "When the dataset has structure components, a SHACL/Turtle "
            "structure is emitted as a sidecar payload (`extras['structure']`, "
            "built by `build_dataset_shacl_turtle`)."
        ),
    ),
    FieldMapping(
        "transform_to_dataservice",
        "DataServiceInputModel",
        "Data service - Dataspot API to I14Y dataservice",
    ),
    FieldMapping(
        "transform_to_concept",
        "CodeListConceptInput",
        "Concept - Dataspot enumeration to I14Y code list",
        note=(
            "Code-list entries are emitted as a sidecar payload "
            "(`extras['entries']`, built by `_build_entries`): each entry maps "
            "`code`, `name` (long/short text), `description`, validity bounds "
            "and `parentCode`."
        ),
    ),
    FieldMapping(
        "_map_distribution",
        "DcatDistributionInputModel",
        "Distribution - Dataspot distribution row to I14Y distribution",
        roots=("raw",),
    ),
]

_FIELD_INTRO = (
    "How each I14Y model field is populated from a Dataspot source record. "
    "**I14Y field** is the model constructor keyword. **Dataspot source** "
    "lists the source fields read from the record; `(child)` marks a 1:n "
    "child table, `(param)` a pipeline parameter, and a quoted/bare value is "
    "a constant or computed expression. **Transform** lists the helper "
    "functions applied (the `dcat.` module prefix is dropped; `mappings.*` is "
    "kept as it points at the vocabulary tables below). A `-` in *Transform* "
    "with a `-` source means the field is computed by the listed helper - see "
    "that helper's docstring for specifics."
)


def _dotted(node: ast.expr) -> str | None:
    """Dotted name of a call target, e.g. ``dcat.multi_language``."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return None


def _str_constant(node: ast.expr | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _subscript_key(node: ast.Subscript) -> str | None:
    sl = node.slice
    if isinstance(sl, ast.Index):  # pragma: no cover - py<3.9 shape
        sl = sl.value
    return _str_constant(sl)


def _norm_transform(name: str) -> str:
    return name[len("dcat."):] if name.startswith("dcat.") else name


def _short_unparse(node: ast.expr, limit: int = 80) -> str:
    text = " ".join(ast.unparse(node).split())
    return text if len(text) <= limit else text[: limit - 1] + "..."


def _build_locals_map(func: ast.FunctionDef) -> dict[str, ast.expr]:
    """Map locally assigned names to their value expression."""
    locals_map: dict[str, ast.expr] = {}
    for stmt in ast.walk(func):
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    locals_map[target.id] = stmt.value
        elif (
            isinstance(stmt, ast.AnnAssign)
            and isinstance(stmt.target, ast.Name)
            and stmt.value is not None
        ):
            locals_map[stmt.target.id] = stmt.value
    return locals_map


def _param_names(func: ast.FunctionDef) -> set[str]:
    a = func.args
    names = {
        arg.arg
        for arg in (*a.posonlyargs, *a.args, *a.kwonlyargs)
    }
    if a.vararg:
        names.add(a.vararg.arg)
    if a.kwarg:
        names.add(a.kwarg.arg)
    return names


def _analyze_value(
    value: ast.expr,
    locals_map: dict[str, ast.expr],
    primary_roots: set[str],
    params: set[str],
) -> tuple[str, str]:
    """Return ``(source_cell, transform_cell)`` for one model keyword value.

    Walks the value expression, following references to locally assigned
    variables, and classifies what it finds into source fields, child
    tables, parameters, constants and applied transform functions.
    """
    record_keys: list[str] = []
    child_keys: list[str] = []
    param_refs: list[str] = []
    const_refs: list[str] = []
    str_consts: list[str] = []
    transforms: list[str] = []
    # Every ``*.get("X")`` key, regardless of base. Root keys also land in
    # record/child_keys; keys on comprehension/lambda vars (e.g. ``d.get``
    # in a sort key) are dropped, but either way they must not resurface as
    # bare string literals in the source cell.
    get_keys: set[str] = set()

    seen_locals: set[str] = set()
    work = [value]
    while work:
        for node in ast.walk(work.pop()):
            if isinstance(node, ast.Call):
                is_get = (
                    isinstance(node.func, ast.Attribute)
                    and node.func.attr == "get"
                    and isinstance(node.func.value, ast.Name)
                )
                if is_get:
                    key = _str_constant(node.args[0]) if node.args else None
                    if key is not None:
                        get_keys.add(key)
                        base = node.func.value.id  # type: ignore[union-attr]
                        if base in (primary_roots | {"children"}):
                            _add(child_keys if base == "children" else record_keys, key)
                else:
                    name = _dotted(node.func)
                    short = name.rsplit(".", 1)[-1] if name else ""
                    if (
                        name
                        and not name.endswith(".get")
                        and name not in _EXCLUDE_CALLS
                        and short not in _EXCLUDE_CALLS
                    ):
                        _add(transforms, _norm_transform(name))
            elif (
                isinstance(node, ast.Subscript)
                and isinstance(node.value, ast.Name)
                and node.value.id in (primary_roots | {"children"})
            ):
                key = _subscript_key(node)
                if key is not None:
                    base = node.value.id
                    _add(child_keys if base == "children" else record_keys, key)
            elif isinstance(node, ast.Name):
                if node.id in locals_map and node.id not in seen_locals:
                    seen_locals.add(node.id)
                    work.append(locals_map[node.id])
                elif node.id in params and node.id not in _STRUCTURAL_PARAMS:
                    _add(param_refs, node.id)
                elif node.id.isupper():
                    _add(const_refs, node.id)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                str_consts.append(node.value)

    skip = set(record_keys) | set(child_keys) | get_keys
    parts = [f"`{_esc(k)}`" for k in record_keys]
    parts += [f"`{_esc(k)}` (child)" for k in child_keys]
    parts += [f"`{p}` (param)" for p in param_refs]
    parts += [f"`{c}`" for c in const_refs]
    for text in str_consts:
        if text and text not in skip:
            _add(parts, f'`"{_esc(text)}"`')

    transform_cell = ", ".join(transforms) if transforms else "-"
    if parts:
        source_cell = ", ".join(parts)
    elif transforms:
        source_cell = "-"
    else:
        source_cell = f"`{_esc(_short_unparse(value))}`"
    return source_cell, transform_cell


def _find_function(tree: ast.Module, name: str) -> ast.FunctionDef:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise SystemExit(f"Function {name!r} not found in {TRANSFORM_PY}")


def _find_model_call(func: ast.FunctionDef, model: str) -> ast.Call:
    for node in ast.walk(func):
        if isinstance(node, ast.Call) and _dotted(node.func) == model:
            return node
    raise SystemExit(f"No {model}(...) call found in {func.name}")


def _docstring_summary(func: ast.FunctionDef) -> str:
    doc = ast.get_docstring(func) or ""
    first_paragraph = doc.split("\n\n", 1)[0]
    return " ".join(first_paragraph.split())


def _warn_undocumented_funcs(tree: ast.Module, documented: set[str]) -> None:
    for node in tree.body:
        if (
            isinstance(node, ast.FunctionDef)
            and node.name.startswith("transform_to_")
            and node.name not in documented
        ):
            print(
                f"warning: {node.name} is a transform but is not listed in "
                "FIELD_MAPPINGS - it will not be documented.",
                file=sys.stderr,
            )


def _render_field_section(tree: ast.Module) -> list[str]:
    sections = ["## Field mappings", _FIELD_INTRO]
    documented: set[str] = set()
    for fm in FIELD_MAPPINGS:
        func = _find_function(tree, fm.func)
        documented.add(fm.func)
        call = _find_model_call(func, fm.model)
        locals_map = _build_locals_map(func)
        params = _param_names(func)
        primary_roots = set(fm.roots) - {"children"}

        rows = ["| I14Y field | Dataspot source | Transform |", "| --- | --- | --- |"]
        for keyword in call.keywords:
            if keyword.arg is None:  # **kwargs splat - nothing to name
                continue
            source, transform = _analyze_value(
                keyword.value, locals_map, primary_roots, params
            )
            rows.append(f"| `{keyword.arg}` | {source} | {transform} |")

        sections.append(f"### {fm.title}")
        summary = _docstring_summary(func)
        if summary:
            sections.append(summary)
        sections.append(f"_Built by `{fm.func}()` -> `{fm.model}`._")
        sections.append("\n".join(rows))
        if fm.note:
            sections.append(f"> {fm.note}")

    _warn_undocumented_funcs(tree, documented)
    return sections


# --------------------------------------------------------------------------
# Vocabulary mappings (mappings.py)
# --------------------------------------------------------------------------
_NONCODE = {
    tokenize.COMMENT,
    tokenize.NL,
    tokenize.NEWLINE,
    tokenize.INDENT,
    tokenize.DEDENT,
    tokenize.ENCODING,
    tokenize.ENDMARKER,
}


@dataclass(frozen=True)
class VocabMapping:
    """One vocabulary table in ``mappings.py`` to document.

    ``labeled_dict``: ``dict[str, str]`` whose entries carry a
    ``# <source label> -> <target label>`` trailing comment.
    ``priority_list``: ordered ``list[tuple[str, str]]`` whose row order
    encodes precedence.
    """

    var: str
    title: str
    kind: str = "labeled_dict"


VOCAB_MAPPINGS: list[VocabMapping] = [
    VocabMapping("_TOPIC_TO_THEME", "Dataspot topic (BFS) to I14Y dataset theme"),
    VocabMapping(
        "_GEOCATEGORY_TO_THEME",
        "Dataspot geoCategory (eCH-166) to I14Y dataset theme",
    ),
    VocabMapping(
        "_PERSONAL_DATA_PRIORITY",
        "Dataspot personal_data to I14Y confidentialityPerson",
        kind="priority_list",
    ),
]

_VOCAB_INTRO = (
    "Controlled-vocabulary code tables applied by the transforms above. "
    "Labels are taken verbatim from the trailing comments in `mappings.py`."
)


def _collect_comments(path: Path) -> tuple[dict[int, str], dict[int, str]]:
    """Return ``(trailing, standalone)`` comment text keyed by line number."""
    with tokenize.open(path) as handle:
        tokens = list(tokenize.generate_tokens(handle.readline))

    code_rows = {tok.start[0] for tok in tokens if tok.type not in _NONCODE}
    trailing: dict[int, str] = {}
    standalone: dict[int, str] = {}
    for tok in tokens:
        if tok.type != tokenize.COMMENT:
            continue
        text = tok.string.lstrip("#").strip()
        target = trailing if tok.start[0] in code_rows else standalone
        target[tok.start[0]] = text
    return trailing, standalone


def _find_assign(tree: ast.Module, var: str) -> ast.Assign:
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == var for t in node.targets
        ):
            return node
    raise SystemExit(f"Mapping variable {var!r} not found in {MAPPINGS_PY}")


def _leading_description(standalone: dict[int, str], assign_line: int) -> str:
    """Join the standalone comment block immediately above ``assign_line``."""
    lines: list[str] = []
    row = assign_line - 1
    while row in standalone:
        lines.append(standalone[row])
        row -= 1
    return " ".join(reversed(lines))


def _split_label(comment: str) -> tuple[str, str]:
    """Split a ``<source> -> <target>`` comment into its two sides."""
    for sep in ("->", "→"):  # ASCII arrow or U+2192
        if sep in comment:
            left, right = comment.split(sep, 1)
            return left.strip(), right.strip()
    return comment.strip(), ""


def _render_labeled_dict(node: ast.Assign, trailing: dict[int, str]) -> str:
    mapping = node.value
    if not isinstance(mapping, ast.Dict):
        raise SystemExit(f"Expected a dict literal for {node.targets}")
    rows = [
        "| Dataspot code | Dataspot label | I14Y code | I14Y label |",
        "| --- | --- | --- | --- |",
    ]
    for key_node, val_node in zip(mapping.keys, mapping.values):
        key = ast.literal_eval(key_node)
        val = ast.literal_eval(val_node)
        src_label, tgt_label = _split_label(trailing.get(val_node.lineno, ""))
        rows.append(
            f"| `{_esc(key)}` | {_esc(src_label)} | `{_esc(val)}` | {_esc(tgt_label)} |"
        )
    return "\n".join(rows)


def _render_priority_list(node: ast.Assign) -> str:
    items = node.value
    if not isinstance(items, ast.List):
        raise SystemExit(f"Expected a list literal for {node.targets}")
    rows = [
        "| Priority | Dataspot value | I14Y code |",
        "| --- | --- | --- |",
    ]
    for priority, elt in enumerate(items.elts, start=1):
        source, target = ast.literal_eval(elt)
        rows.append(f"| {priority} | `{_esc(source)}` | `{_esc(target)}` |")
    return "\n".join(rows)


def _warn_undocumented_vocab(tree: ast.Module, documented: set[str]) -> None:
    """Warn about constant dict/list tables that nobody documents."""
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(
            node.value, (ast.Dict, ast.List)
        ):
            continue
        for target in node.targets:
            if (
                isinstance(target, ast.Name)
                and target.id.isupper()
                and target.id not in documented
            ):
                print(
                    f"warning: {target.id} looks like a vocabulary table but is "
                    "not listed in VOCAB_MAPPINGS - it will not be documented.",
                    file=sys.stderr,
                )


def _render_vocab_section(
    tree: ast.Module, trailing: dict[int, str], standalone: dict[int, str]
) -> list[str]:
    sections = ["## Vocabulary mappings", _VOCAB_INTRO]
    documented: set[str] = set()
    for mapping in VOCAB_MAPPINGS:
        node = _find_assign(tree, mapping.var)
        documented.add(mapping.var)

        sections.append(f"### {mapping.title}")
        description = _leading_description(standalone, node.lineno)
        if description:
            sections.append(description)
        sections.append(f"_Source: `{mapping.var}`._")
        if mapping.kind == "labeled_dict":
            sections.append(_render_labeled_dict(node, trailing))
        elif mapping.kind == "priority_list":
            sections.append(_render_priority_list(node))
        else:  # pragma: no cover - guarded by VocabMapping usage
            raise SystemExit(f"Unknown mapping kind {mapping.kind!r}")

    _warn_undocumented_vocab(tree, documented)
    return sections


# --------------------------------------------------------------------------
# Document assembly
# --------------------------------------------------------------------------
_HEADER = """<!--
GENERATED FILE - DO NOT EDIT BY HAND.
Regenerate with: uv run python scripts/gen_mapping_docs.py
Sources of truth:
  src/metadataswiss_connector/sources/dataspot/transform.py
  src/metadataswiss_connector/sources/dataspot/mappings.py
-->

# Dataspot to I14Y mappings

How Dataspot records are mapped into the I14Y DCAT format: the field-level
mapping per record type, then the controlled-vocabulary code tables. Run
`uv run python scripts/gen_mapping_docs.py` to refresh.
"""


def build_markdown() -> str:
    transform_tree = ast.parse(TRANSFORM_PY.read_text(encoding="utf-8"))
    mapping_tree = ast.parse(MAPPINGS_PY.read_text(encoding="utf-8"))
    trailing, standalone = _collect_comments(MAPPINGS_PY)

    sections = [_HEADER.rstrip()]
    sections += _render_field_section(transform_tree)
    sections += _render_vocab_section(mapping_tree, trailing, standalone)
    return "\n\n".join(sections).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate docs/mappings.md")
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 (without writing) if docs/mappings.md is out of date",
    )
    args = parser.parse_args()

    markdown = build_markdown()
    relative = OUTPUT_MD.relative_to(REPO_ROOT).as_posix()

    if args.check:
        current = OUTPUT_MD.read_text(encoding="utf-8") if OUTPUT_MD.exists() else ""
        if current != markdown:
            print(
                f"{relative} is out of date. "
                "Run: uv run python scripts/gen_mapping_docs.py",
                file=sys.stderr,
            )
            return 1
        print(f"{relative} is up to date.")
        return 0

    OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_MD.write_text(markdown, encoding="utf-8", newline="\n")
    print(f"Wrote {relative}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

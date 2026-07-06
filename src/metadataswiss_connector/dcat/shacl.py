"""Source-neutral SHACL/Turtle builder for I14Y dataset structure imports.

I14Y accepts a SHACL Turtle document at
``POST /datasets/{id}/structures/imports`` describing a dataset's
attributes (columns). This module renders that document from a list of
``StructureComponent`` dicts — one per attribute — without knowing
anything about where the components came from.

Per-source adapters (e.g. ``sources/dataspot/structure.py``) translate
their own raw rows into ``StructureComponent`` dicts and call
``build_shacl_turtle`` from here. To wire up a second source, write that
adapter — no changes to this file should be needed.

The Turtle is hand-built rather than via rdflib to avoid a heavy
dependency: the output shape is small, deterministic, and only needs to
escape string literals correctly.
"""

from __future__ import annotations

import os
from typing import TypedDict


# I14Y concept IRI base used for ``dcterms:conformsTo`` on code-list-typed
# attributes. The source's enumeration UUID is published as ``identifier``
# on the corresponding I14Y concept, so ``<base>/<id>`` resolves to the
# concept page. Env-configurable because abnahme and prod live on
# different hosts; default targets abnahme.
I14Y_CONCEPT_IRI_BASE = os.environ["I14Y_IRI_BASE"].rstrip("/")

class StructureComponent(TypedDict, total=False):
    """One ``sh:property`` block worth of fields.

    ``path_iri`` and ``xsd_datatype`` are required; all other keys are
    optional and emitted only when present. ``min_count``/``max_count``
    follow SHACL semantics (``min_count=0`` means optional, ``max_count=1``
    means single-valued). ``conforms_to_iri`` is typically the I14Y
    concept IRI for a code-list-typed attribute.
    """

    path_iri: str
    name: str | None
    description: str | None
    xsd_datatype: str
    conforms_to_iri: str | None
    order: int | None
    min_count: int | None
    max_count: int | None
    min_inclusive: float | int | None
    max_inclusive: float | int | None


_PREFIXES = """\
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
"""


def build_shacl_turtle(
    *,
    subject_iri: str,
    subject_label: str | None,
    components: list[StructureComponent],
) -> str | None:
    """Render a SHACL ``NodeShape`` for ``subject_iri``.

    Returns ``None`` when ``components`` is empty, so callers can skip
    the upload entirely. Component order in the input list determines
    block order in the output; sort upstream if you need a specific
    ``sh:order`` sequence.
    """
    if not components:
        return None

    lines: list[str] = [_PREFIXES, ""]
    lines.append(f"<{subject_iri}> a sh:NodeShape ;")
    if subject_label:
        lines.append(f"    rdfs:label {_literal(subject_label)} ;")

    blocks = [_property_block(c) for c in components]
    for idx, block in enumerate(blocks):
        terminator = " ;" if idx < len(blocks) - 1 else " ."
        lines.append(f"    sh:property {block}{terminator}")

    return "\n".join(lines) + "\n"


def _property_block(component: StructureComponent) -> str:
    """Render one ``sh:property [ ... ]`` block."""
    parts: list[str] = [f"sh:path <{component['path_iri']}>"]

    name = component.get("name")
    if name:
        parts.append(f"sh:name {_literal(name)}")

    description = component.get("description")
    if description:
        parts.append(f"dcterms:description {_literal(description)}")

    parts.append(f"sh:datatype {component['xsd_datatype']}")

    conforms_to = component.get("conforms_to_iri")
    if conforms_to:
        parts.append(f"dcterms:conformsTo <{conforms_to}>")

    order = component.get("order")
    if isinstance(order, int):
        parts.append(f"sh:order {order}")

    min_count = component.get("min_count")
    if isinstance(min_count, int):
        parts.append(f"sh:minCount {min_count}")
    max_count = component.get("max_count")
    if isinstance(max_count, int):
        parts.append(f"sh:maxCount {max_count}")

    min_inclusive = component.get("min_inclusive")
    if min_inclusive is not None:
        parts.append(f"sh:minInclusive {_numeric(min_inclusive)}")
    max_inclusive = component.get("max_inclusive")
    if max_inclusive is not None:
        parts.append(f"sh:maxInclusive {_numeric(max_inclusive)}")

    body = " ;\n        ".join(parts)
    return "[\n        " + body + "\n    ]"


def _numeric(value: float | int | bool) -> str:
    """Render a numeric constraint as a Turtle literal.

    Integers stay bare (``0``); fractional values get a decimal point.
    Turtle's numeric shorthand requires a digit on each side of the dot.
    """
    if isinstance(value, bool):
        # Avoid bool being treated as int below
        return "true" if value else "false"
    if isinstance(value, int) or (isinstance(value, float) and value.is_integer()):
        return str(int(value))
    return repr(float(value))


def _literal(text: str) -> str:
    """Render a Python string as a Turtle string literal.

    Escapes the minimal set required by Turtle: backslash, quote, and
    control characters. Newlines/tabs are escaped rather than relying on
    triple-quoted strings — keeps the output one logical line per value.
    """
    escaped = (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'

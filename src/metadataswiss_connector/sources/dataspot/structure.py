"""Build a SHACL Turtle document from Dataspot structure components.

Each Dataspot dataset becomes one ``sh:NodeShape`` whose ``sh:property``
blocks describe the dataset's attributes (columns). The result is
uploaded to I14Y's ``POST /datasets/{id}/structures/imports`` endpoint.

The Turtle is hand-built rather than via rdflib to avoid a heavy
dependency: the output shape is small, deterministic, and only needs to
escape string literals correctly.
"""

from __future__ import annotations

from metadataswiss_connector.sources.dataspot.constants import (
    DATASPOT_BASETYPE_DEFAULT_XSD,
    DATASPOT_BASETYPE_TO_XSD,
    DATASPOT_DATATYPE_LABEL_TO_XSD,
    DATASPOT_IRI_BASE,
    DATATYPE_KIND_ENUMERATION,
)

_PREFIXES = """\
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
"""


def build_dataset_shacl_turtle(
    *, dataset_id: str, dataset_label: str | None, components: list[dict]
) -> str | None:
    """Return a SHACL Turtle string describing the dataset's structure.

    Returns ``None`` when the dataset has no public structure components,
    so callers can skip the upload entirely.

    ``components`` rows come from the ``dataset_structure_components``
    source table; they're sorted here by ``order`` so the resulting
    ``sh:order`` values are stable.
    """
    rows = [c for c in components if c.get("attribute_id")]
    if not rows:
        return None
    rows.sort(key=lambda c: (c.get("order") or 0, c.get("composition_id") or ""))

    lines: list[str] = [_PREFIXES, ""]
    shape_iri = f"<{DATASPOT_IRI_BASE}/datasets/{dataset_id}>"
    lines.append(f"{shape_iri} a sh:NodeShape ;")
    if dataset_label:
        lines.append(f"    rdfs:label {_literal(dataset_label)} ;")

    property_blocks = [_property_block(row) for row in rows]
    for idx, block in enumerate(property_blocks):
        terminator = " ;" if idx < len(property_blocks) - 1 else " ."
        lines.append(f"    sh:property {block}{terminator}")

    return "\n".join(lines) + "\n"


def _property_block(component: dict) -> str:
    """Render one ``sh:property [ ... ]`` block for an attribute row."""
    attribute_id = component["attribute_id"]
    path_iri = f"<{DATASPOT_IRI_BASE}/attributes/{attribute_id}>"

    parts: list[str] = [f"sh:path {path_iri}"]

    name = component.get("composition_label") or component.get("attribute_label")
    if name:
        parts.append(f"sh:name {_literal(name)}")

    description = (
        component.get("composition_description")
        or component.get("attribute_description")
    )
    if description:
        parts.append(f"dcterms:description {_literal(description)}")

    parts.append(f"sh:datatype {_xsd_datatype(component)}")

    # Code-list-typed attributes carry a conformsTo pointing at the
    # Dataspot enumeration IRI. The enumeration id is the same UUID we
    # publish as ``identifier`` on the I14Y concept, so consumers can
    # bridge structure → concept without an extra lookup.
    if component.get("datatype_kind") == DATATYPE_KIND_ENUMERATION and component.get(
        "datatype_id"
    ):
        conforms_iri = f"<{DATASPOT_IRI_BASE}/enumerations/{component['datatype_id']}>"
        parts.append(f"dcterms:conformsTo {conforms_iri}")

    order = component.get("order")
    if isinstance(order, int):
        parts.append(f"sh:order {order}")

    # Required: attribute is authoritative per dataspot model; the
    # composition may carry a customProperties.required override but we
    # treat it as a fallback only.
    required = component.get("attribute_required") or component.get(
        "composition_required"
    )
    if (required or "").upper() == "MANDATORY":
        parts.append("sh:minCount 1")
    else:
        parts.append("sh:minCount 0")

    cardinality = (component.get("attribute_cardinality") or "").upper()
    if cardinality == "ONE":
        parts.append("sh:maxCount 1")

    min_inclusive = component.get("attribute_min_inclusive")
    if min_inclusive is not None:
        parts.append(f"sh:minInclusive {_numeric(min_inclusive)}")
    max_inclusive = component.get("attribute_max_inclusive")
    if max_inclusive is not None:
        parts.append(f"sh:maxInclusive {_numeric(max_inclusive)}")

    body = " ;\n        ".join(parts)
    return "[\n        " + body + "\n    ]"


def _xsd_datatype(component: dict) -> str:
    """Resolve a component's XSD datatype IRI.

    Precedence: explicit baseType → datatype label fallback → string.
    Enumeration-typed attributes also land here; we still emit
    ``xsd:string`` for them so consumers without code-list awareness get
    a usable type, while ``dcterms:conformsTo`` carries the semantics.
    """
    base_type = (component.get("datatype_base_type") or "").upper()
    if base_type in DATASPOT_BASETYPE_TO_XSD:
        return DATASPOT_BASETYPE_TO_XSD[base_type]
    label = (component.get("datatype_label") or "").strip().lower()
    if label in DATASPOT_DATATYPE_LABEL_TO_XSD:
        return DATASPOT_DATATYPE_LABEL_TO_XSD[label]
    return DATASPOT_BASETYPE_DEFAULT_XSD


def _numeric(value: float) -> str:
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

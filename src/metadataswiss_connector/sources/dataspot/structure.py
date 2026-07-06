"""Adapter: Dataspot structure rows → I14Y SHACL Turtle.

Translates rows from the ``dataset_structure_components`` raw table into
the source-neutral ``StructureComponent`` shape consumed by
``dcat.shacl.build_shacl_turtle``. Keep the *what* (Dataspot field
mapping, XSD resolution, minCount/maxCount semantics) here; the *how*
(Turtle rendering) lives in ``dcat/shacl.py``.
"""

from __future__ import annotations

from metadataswiss_connector.dcat.shacl import (
    I14Y_CONCEPT_IRI_BASE,
    StructureComponent,
    build_shacl_turtle,
)
from metadataswiss_connector.sources.dataspot.constants import (
    DATASPOT_BASETYPE_DEFAULT_XSD,
    DATASPOT_BASETYPE_TO_XSD,
    DATASPOT_DATATYPE_LABEL_TO_XSD,
    SOURCES__DATASPOT__BASE_URL,
    DATATYPE_KIND_ENUMERATION,
)


def build_dataset_shacl_turtle(
    *, dataset_id: str, dataset_label: str | None, components: list[dict]
) -> str | None:
    """Return a SHACL Turtle document for one Dataspot dataset, or ``None``.

    ``components`` are raw rows from ``dataset_structure_components``;
    rows without ``attribute_id`` are skipped. The result is uploaded to
    I14Y's ``POST /datasets/{id}/structures/imports`` endpoint.
    """
    rows = [c for c in components if c.get("attribute_id")]
    if not rows:
        return None
    rows.sort(key=lambda c: (c.get("order") or 0, c.get("composition_id") or ""))

    return build_shacl_turtle(
        subject_iri=f"{SOURCES__DATASPOT__BASE_URL}/datasets/{dataset_id}",
        subject_label=dataset_label,
        components=[_to_structure_component(r) for r in rows],
    )


def _to_structure_component(row: dict) -> StructureComponent:
    """Map one Dataspot row to a neutral ``StructureComponent``."""
    component: StructureComponent = {
        "path_iri": f"{SOURCES__DATASPOT__BASE_URL}/attributes/{row['attribute_id']}",
        "name": row.get("composition_label") or row.get("attribute_label"),
        "description": (
            row.get("composition_description") or row.get("attribute_description")
        ),
        "xsd_datatype": _xsd_datatype(row),
    }

    # Code-list-typed attributes carry a conformsTo pointing at the I14Y
    # concept IRI. The Dataspot enumeration UUID is published as
    # ``identifier`` on the corresponding I14Y concept, so the same id
    # appended to the I14Y concept IRI base resolves directly to the
    # concept page (no extra lookup needed).
    if row.get("datatype_kind") == DATATYPE_KIND_ENUMERATION and row.get("datatype_id"):
        component["conforms_to_iri"] = (
            f"{I14Y_CONCEPT_IRI_BASE}/{row['datatype_id']}"
        )

    order = row.get("order")
    if isinstance(order, int):
        component["order"] = order

    # Required: attribute is authoritative per dataspot model; the
    # composition may carry a customProperties.required override but we
    # treat it as a fallback only.
    required = row.get("attribute_required") or row.get("composition_required")
    component["min_count"] = 1 if (required or "").upper() == "MANDATORY" else 0

    if (row.get("attribute_cardinality") or "").upper() == "ONE":
        component["max_count"] = 1

    if row.get("attribute_min_inclusive") is not None:
        component["min_inclusive"] = row["attribute_min_inclusive"]
    if row.get("attribute_max_inclusive") is not None:
        component["max_inclusive"] = row["attribute_max_inclusive"]

    return component


def _xsd_datatype(row: dict) -> str:
    """Resolve a Dataspot row's XSD datatype IRI.

    Precedence: explicit baseType → datatype label fallback → string.
    Enumeration-typed attributes also land here; we still emit
    ``xsd:string`` for them so consumers without code-list awareness get
    a usable type, while ``dcterms:conformsTo`` carries the semantics.
    """
    base_type = (row.get("datatype_base_type") or "").upper()
    if base_type in DATASPOT_BASETYPE_TO_XSD:
        return DATASPOT_BASETYPE_TO_XSD[base_type]
    label = (row.get("datatype_label") or "").strip().lower()
    if label in DATASPOT_DATATYPE_LABEL_TO_XSD:
        return DATASPOT_DATATYPE_LABEL_TO_XSD[label]
    return DATASPOT_BASETYPE_DEFAULT_XSD

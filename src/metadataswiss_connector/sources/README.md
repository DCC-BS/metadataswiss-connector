# Catalog sources

Each subfolder of `sources/` is one catalog source (e.g. Dataspot,
CKAN, …). A source describes **what** to extract (a dlt source factory)
and **how** to map each of its raw resources to an I14Y DCAT model.

Sources are plugged in via `registry.CatalogSource` and surface in the
Dagster pipeline through `dagster_defs/`.

## What a new source must provide

A new source `sources/<name>/` is expected to expose a single
`CatalogSource` instance — usually from its `__init__.py` — wiring up:

| Field                  | What it does                                                                                                       |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `name`                 | Stable identifier; also the dlt pipeline name and the DuckDB schema prefix (`<name>_raw`).                         |
| `dlt_source_factory`   | Zero-arg callable returning a configured `DltSource`. A factory (not an instance) lets each run pick up fresh creds. |
| `resources`            | `dict[str, ResourceSpec]` — one entry per raw dlt resource you want to publish to I14Y.                            |
| `publisher` (optional) | Per-source publisher override; falls back to the global `I14Y_PUBLISHER_IDENTIFIER`.                               |

`ResourceSpec(transform, kind, lookups, sibling_parent)`:

- **`transform`** — function with signature
  `(record, children, *, lookups, publisher) -> BaseModel | (BaseModel, extras)`.
  Maps one raw row to an I14Y input model (see `dcat/i14y_models.py`).
  Validation errors are caught and logged by `dcat/transforms.py`; you don't need to.
- **`kind`** — `"dataset"` or `"concept"`. Drives sync routing
  (datasets endpoint vs. concepts endpoint, state file).
- **`lookups`** — names of *other* raw dlt resources in the same source
  whose rows your transform reads via the `children` / `lookups`
  arguments. Declared explicitly so Dagster lineage shows the cross-table
  reads as upstream deps; runtime behaviour is unaffected (lookups are
  still discovered dynamically).
- **`sibling_parent`** — name of the *extract* dlt resource that this
  resource's sibling child tables resolve from. dlt names their
  parent-ref column `_<extract resource>_id`, so this must be set when
  the transform reads a filtered transformer table whose children were
  extracted against a differently-named upstream resource (dataspot:
  `data_products` reads `distributions`, which resolve from
  `data_products_all`). Omit it when the children reference this
  resource's own name (dataspot: `code_lists` / `code_list_entries`).

## The transform contract

The transform receives:

- `record: dict` — flattened raw row from DuckDB. dlt's `__` separator is
  preserved on nested fields (e.g. `custom_properties__publisher`).
- `children: dict[str, list]` — dlt 1:n child tables keyed by field name
  (e.g. `"tags"` → `["foo", "bar"]`). For top-level sibling tables
  wired via `resolve` + `include_from_parent`, the table name is the key
  and the value is the list of rows for this parent.
- `lookups: dict[str, list[dict]]` — full top-level tables in the same
  raw dataset that have no parent-ref column. Used for cross-reference
  data (e.g. attribution → role/post/person resolution).
- `publisher: str` — I14Y publisher identifier.

The return value is either the typed model alone, or
`(model, extras)`. `extras` is a JSON-serialisable dict carrying
sidecar payloads that the I14Y model itself can't hold but sync needs to
replay as a follow-up call.

### Known sidecar extras keys

| Key                  | Consumed by sync                              | Shape                                  |
| -------------------- | --------------------------------------------- | -------------------------------------- |
| `"structure"`        | `POST /datasets/{id}/structures/imports`      | SHACL Turtle string (see below).       |
| `"code_list_entries"`| `POST /concepts/{id}/entries`                 | List of `CodeListEntry`-shaped dicts.  |

If you invent a new extras key, document it here and teach `sync.py`
how to replay it.

## Reusable building blocks (use these, don't reinvent)

- **`dcat.builders`** — language wrappers, frequency lookup, theme
  helpers, landing-page mapping, etc.
- **`dcat.shacl.build_shacl_turtle`** — source-neutral SHACL Turtle
  renderer for dataset-structure imports. Feed it a list of
  `StructureComponent` dicts; the adapter (per-source mapping from raw
  rows to `StructureComponent`) is the only part you write. See
  `sources/dataspot/structure.py` for a worked example.
- **`dcat.i14y_models`** — Pydantic input models that mirror the I14Y
  Partner API contract; always import the model rather than building
  dicts by hand so validation catches issues at transform time.

## Folder layout (convention)

```
sources/<name>/
    __init__.py        # exports the CatalogSource instance
    source.py          # dlt source + resource definitions
    transform.py       # raw row → I14Y model mapping
    constants.py       # source-specific constants only — keep I14Y/DCAT
                       # constants in the dcat/ package
    structure.py       # optional: adapter to dcat.shacl (if the source
                       # publishes dataset structures)
    mappings.py        # optional: code/category lookups specific to this source
    enrichment.py      # optional: external lookups (e.g. address registries)
```

Anything that isn't source-specific should live under `dcat/` (rendering,
generic models) or `registry.py` / `resources.py` (orchestration glue) —
not inside the source folder.

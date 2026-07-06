# metadataswiss-connector

Connector for extracting metadata from data catalogs, transforming it into the
[DCAT-AP-CH](https://www.i14y.admin.ch/) format and publishing it to the
federal [Interoperability Platform I14Y](https://www.i14y.admin.ch/).

## Pipeline

The pipeline follows an ELT pattern with three steps, each based on
[dlt](https://dlthub.com/) and orchestrated via [Dagster](https://dagster.io/):

```
Source catalog → [extract] → [transform] → [publish] → I14Y API
```

1. **Extract + Load** — raw data is extracted from the source catalog and
   loaded into a local DuckDB.
2. **Transform** — raw data is mapped into the I14Y-compatible DCAT format.
3. **Publish** — transformed records are reconciled against the I14Y Partner
   API (create/update/delete), tracked per source in a state file.

The orchestration layer is optional: `pipeline.py`, `registry.py`, `sources/`
and `dcat/` carry no Dagster dependency, so any orchestrator can drive the same
`extract`/`transform`/`publish` helpers.

## Where to look

- **[Mappings](mappings.md)** — how each Dataspot source field maps to an I14Y
  model field, plus the controlled-vocabulary code tables. This page is
  generated from the source code; see below.
- **[API reference](reference/index.md)** — module-level reference rendered
  from the docstrings.
- **README** — setup, configuration, running the pipeline and deployment live
  in the [repository README](https://github.com/DCC-BS/metadataswiss-connector#readme).

## Regenerating the mapping documentation

The [Mappings](mappings.md) page is generated from
`sources/dataspot/transform.py` and `sources/dataspot/mappings.py`:

```bash
uv run python scripts/gen_mapping_docs.py            # refresh docs/mappings.md
uv run python scripts/gen_mapping_docs.py --check     # CI: fail if out of date
```

CI runs the `--check` form, so a mapping change that is not accompanied by a
regenerated page fails the build.

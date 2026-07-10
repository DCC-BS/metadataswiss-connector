# metadataswiss-connector

Connector for extracting metadata from data catalogs, transforming it into the [DCAT-AP-CH](https://www.i14y.admin.ch/) format and publishing it to the federal [Interoperability Platform I14Y](https://www.i14y.admin.ch/).

## Architecture

The pipeline follows an ELT pattern with three steps, each based on [dlt](https://dlthub.com/):

1. **Extract + Load** (`extract`): Raw data is extracted from the source catalog via dlt and loaded into a local DuckDB.
2. **Transform** (`transform`): The raw data is transformed into the I14Y-compatible DCAT format and written to a separate dataset.
3. **Publish** (`publish`): The transformed records are synchronized against the I14Y Partner API (create/update/delete), using a per-source state file to track the remote IDs.

```
Source catalog → [extract] → [transform] → [publish] → I14Y API
```

The steps are orchestrated via [Dagster](https://dagster.io/) (see the [Running the pipeline](#running-the-pipeline) section below), but the underlying logic is framework-free: `pipeline.py`, `registry.py`, `config.py`, `sources/` and `dcat/` have no Dagster dependency, so additional orchestrators (Airflow or similar) can drive the same `extract`/`transform`/`publish` helpers without issue.

### Plugin architecture

Catalog sources are registered as framework-free `CatalogSource` descriptors. Dagster iterates over the list in `sources/__init__.py` and materializes the desired assets per source.

### Project structure

```
src/metadataswiss_connector/
├── registry.py                    # CatalogSource, ResourceSpec, TransformFn
├── config.py                      # I14YConfig
├── resources.py                   # dlt pipeline helpers + I14Y client factory
├── sync.py                        # I14Y create/update/delete reconciliation
│                                  #   (sync/purge with kind dispatch)
├── sources/
│   ├── __init__.py                # SOURCES — list of all registered sources
│   └── dataspot/
│       ├── __init__.py            # dataspot_catalog_source (CatalogSource)
│       ├── auth.py                # Azure AD OAuth2 + Dataspot access key
│       ├── source.py              # REST API source definition (dlt)
│       ├── transform.py           # Field mapping Dataspot → DCAT / Concept
│       ├── mappings.py            # Controlled vocabularies (themes, frequency, …)
│       ├── enrichment.py          # Staatskalender lookup, data-owner resolver
│       ├── structure.py           # Dataspot structure rows → StructureComponent adapter
│       └── constants.py           # Constants + TRANSFORM_VERSION
├── pipeline.py                   # Framework-free extract/transform/publish helpers
├── dagster_defs/                 # Dagster entrypoint (assets + jobs per source)
│   ├── __init__.py               #   exports `defs: Definitions`
│   ├── assets.py                 #   asset factory per CatalogSource
│   ├── schedules.py              #   full_sync_schedule (one job per source)
│   ├── email_alerts.py           #   run-failure + invalid-records sensors (email)
│   ├── invalid_records_report.py #   invalid-records alert email rendering
│   └── purge_jobs.py             #   purge-job factory per CatalogSource
└── dcat/
    ├── builders.py                # I14Y DCAT model builder
    ├── transforms.py              # Raw→DCAT transform pipeline step
    ├── duckdb_io.py               # DuckDB read-path for transformed records
    ├── dlt_schema.py              # dlt-schema introspection + re-nesting over DuckDB
    ├── lookups.py                 # Cross-reference lookup tables for a transform run
    ├── shacl.py                   # Source-neutral SHACL/Turtle builder (dataset structure)
    └── i14y_models.py             # Generated Pydantic models from the I14Y OpenAPI spec

packages/i14y-client/              # Standalone Partner API client (separate package)
```

### Connecting a new source

1. **Create a package** under `sources/<name>/` with:
   - `source.py` — dlt source factory (zero-arg callable returning a `DltSource`)
   - `transform.py` — one function per resource with the signature
     `transform_fn(record: dict, children: dict[str, list], *, lookups: Lookups, publisher: str) -> BaseModel | tuple[BaseModel, dict]`,
     which maps a raw record onto the appropriate I14Y input model
     (`DcatDatasetInputModel` for datasets, `CodeListConceptInput` for concepts).
     Return optional sidecar payloads (e.g. code-list entries, SHACL structures) as
     the second tuple element — sync invokes them after create/update as a
     follow-up via `apply_extras`.
   - `__init__.py` — instantiate and export `CatalogSource(name=..., dlt_source_factory=..., resources={<resource>: ResourceSpec(transform_fn, kind="dataset"|"concept")}, transform_version=...)`
2. **Register** it in `sources/__init__.py`: append the `CatalogSource` to `SOURCES`.

When the transform logic of an existing source changes, `transform_version` should be incremented. Records with a differing persisted version are re-published on the next sync, even without a new `modified` date from the source.

## Supported sources

- **Dataspot** — extraction of data products via REST API (e.g. [Datenkatalog Basel-Stadt](https://datenkatalog.bs.ch))

## Requirements

- Python >=3.13 <3.14
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
uv sync
```

### Configuration

All configuration runs through `.env` (see `.env.example` as a template).
`.env` is loaded automatically when the package is imported.

**I14Y Partner API** (publishing target). `I14Y_PUBLISHER_IDENTIFIER` is the
organisation under which records are published; `I14Y_USER_AGENT` is sent on
every request:

```bash
I14Y_BASE_URL=https://api-a.i14y.admin.ch/api/partner/v1
I14Y_TOKEN_URL=https://identity.i14y.a.c.bfs.admin.ch/realms/bfs-sis-a/protocol/openid-connect/token
I14Y_IRI_BASE=https://iri.i14y.a.c.bfs.admin.ch/concept
I14Y_PUBLISHER_IDENTIFIER=CH_KT_BS
I14Y_USER_AGENT=metadataswiss-connector/0.1.0 (Statistisches Amt Basel-Stadt; contact: statistik@bs.ch)
# Secrets — never commit:
I14Y_CLIENT_ID=
I14Y_CLIENT_SECRET=
```

**Dataspot source connection.** dlt resolves these values via the
`SOURCES__DATASPOT__*` env naming (double underscore per TOML level):

```bash
SOURCES__DATASPOT__BASE_URL=https://datenkatalog.bs.ch
SOURCES__DATASPOT__DATABASE_NAME=prod
# Secrets — never commit:
SOURCES__DATASPOT__TENANT_ID=
SOURCES__DATASPOT__CLIENT_ID=
SOURCES__DATASPOT__CLIENT_SECRET=
SOURCES__DATASPOT__DATASPOT_ACCESS_KEY=
SOURCES__DATASPOT__EXPOSED_CLIENT_ID=
```

**Dataspot enrichment** (deployment-specific, no fallback — all required):

```bash
# Role UUID of the "data owner" attribution in Dataspot (tenant-specific)
DATA_OWNER_ROLE_UUID=02222f05-5690-4cb8-8d90-c27ca57e98e9
# Staatskalender API — enriches organisational units with contact data
STAATSKALENDER_BASE_URL=https://staatskalender.bs.ch/api
# Fixed contacts for responsiblePerson / responsibleDeputy on every code-list concept, dataset and dataservice
RESPONSIBLE_PERSON_EMAIL=email@example.com
RESPONSIBLE_DEPUTY_EMAIL=email@example.com
```

## Running the pipeline

The pipeline is orchestrated via Dagster. Assets are generated per registered source and resource (`<source>_raw → <source>_<resource>_transformed → <source>_<resource>_published`), so that individual steps can be re-materialized in a targeted manner and the lineage is visible in the UI.

```bash
# Start the Dagster UI (default: http://localhost:3000)
uv run dagster dev
```

The `metadataswiss_connector.dagster_defs` module is loaded automatically by `dagster dev` via `[tool.dagster]` in `pyproject.toml`. A `full_sync_schedule` (daily 03:00, timezone `Europe/Zurich`, configurable via `CONNECTOR_SYNC_CRON` / `CONNECTOR_SYNC_TIMEZONE`) is registered but starts **stopped** — it must be deliberately enabled in the Dagster UI under *Automation*.

## Deployment (Docker Compose)

For production operation a Compose setup with four services is included: `postgres` (Dagster storage), `connector_code` (gRPC code server — **all runs execute here**, which is why the persistent data volume is attached to this container), `webserver` (UI on port 3000) and `daemon` (schedules, run queue, sensors).

```bash
# 1. Populate .env with the PRODUCTION credentials (see .env.example)
cp .env.example .env && $EDITOR .env

# 2. Build and start
docker compose up -d --build

# 3. Open the UI
open http://localhost:3000
```

Persistence:
- **Dagster metadata** resides in the named volume `postgres_data`.
- **Connector state** (DuckDB warehouse + I14Y state files) resides via a **bind mount** on the host — the container thus uses the same files as a local `dagster dev`. The host paths are configurable via `.env`:
  - `HOST_STATE_DIR` (default `./data/state`) → `/mnt/state` in the container (`CONNECTOR_DATA_DIR`)
  - `HOST_DUCKDB_DIR` (default `.`, repo root) + `DUCKDB_FILENAME` (default `metadata.duckdb`) → `/mnt/duckdb/<file>` in the container (`DUCKDB_PATH`)

The app secrets come from `.env`; Postgres credentials can be overridden via `DAGSTER_PG_*`.

> **Switching environments (acceptance → prod):** The state files map source IDs to I14Y UUIDs of a *specific* environment. When switching, point `HOST_STATE_DIR` (and possibly `HOST_DUCKDB_DIR`) at an **empty, prod-owned** directory — never reuse the acceptance state files, otherwise the sync will try to update/delete UUIDs that do not exist in prod.

After the first start: enable the sensors (`email_on_run_failure`, `email_on_invalid_records`) and, if applicable, the `full_sync_schedule` in the UI. For the first prod run it is recommended to manually materialize the `*_published` asset rather than arming the schedule directly.

#### Email alerts on run failures

A `run_failure_sensor` (`email_on_run_failure` in `dagster_defs/email_alerts.py`) sends an email to the recipients configured in `EMAIL_TO` on every failed Dagster run. The body contains the run ID, job name and error text; the full event log is attached as `run_<id>.log`.

SMTP configuration via `.env` (see `.env.example`):

```
SMTP_HOST=smtp.example.ch
SMTP_PORT=587
SMTP_USER=alerts@example.ch
SMTP_PASSWORD=...
SMTP_SECURITY=starttls  # starttls (port 587) | ssl (port 465)
SMTP_FROM=alerts@example.ch
EMAIL_TO=ops@example.ch,team@example.ch
```

If `SMTP_HOST`, `SMTP_FROM` or `EMAIL_TO` are missing, the sensor is silently skipped — the sensor must be enabled in the Dagster UI under *Sensors* for it to fire.

### Purge

Remove all active records of a source from I14Y (soft-delete in the state file). For each registered source there is a Dagster job `<source>_purge` (e.g. `dataspot_purge`), started via the Dagster UI under *Jobs*. The default config is a dry run; for an actual deletion in the Launchpad:

```yaml
ops:
  dataspot_purge_op:
    config:
      dry_run: false
      confirm: DELETE
```

Without `confirm: DELETE` when `dry_run: false`, the run fails intentionally. Results (`deleted`, `failed`, `per_resource`) appear as op metadata in the run log.

### Results

The results reside in the local DuckDB warehouse (default `data/metadata.duckdb`, configurable via `DUCKDB_PATH`):
- Dataset `<source>_raw` — raw data from the respective source (one table per dlt resource)
- Dataset `i14y_dcat` — transformed data in DCAT format (all sources, one table per resource)

Remote state is persisted per (source, resource) in `data/state/<source>_<resource>_ids.json` (mapping source ID → I14Y UUID, including `modified` timestamp and `transform_version`).

## Regenerating the I14Y models

The Pydantic models in `src/metadataswiss_connector/dcat/i14y_models.py` are generated automatically from the I14Y OpenAPI specification (`docs/i14y_rest_api.json`). When updating the spec:

```bash
uv run datamodel-codegen \
  --input docs/i14y_rest_api.json \
  --input-file-type openapi \
  --output src/metadataswiss_connector/dcat/i14y_models.py \
  --output-model-type pydantic_v2.BaseModel \
  --snake-case-field \
  --use-field-description \
  --field-constraints \
  --target-python-version 3.13 \
  --use-standard-collections \
  --use-union-operator \
  --allow-population-by-field-name
```

The file is generated — do not edit it by hand. Contract violations in `builders.py` or `transform_to_dataset` are reported after regeneration by the type checker or at runtime by Pydantic.

## Tests

The unit tests cover the pure transformation layer (Dataspot record → I14Y model): `dcat/builders.py`, `sources/dataspot/mappings.py` and `sources/dataspot/transform.py`. These functions are testable without network/IO, so Dataspot does **not** need to be mocked — the inputs come from versioned JSON fixtures under `tests/fixtures/`.

```bash
uv run pytest
```

The extraction layer (`source.py`/`auth.py`, dlt REST API against Dataspot) is deliberately not covered: mocking the entire API tests dlt more than our code and is more maintenance-intensive. New domain logic in the transformation should be accompanied by a fixture-based test case.

## License

See [LICENSE](LICENSE).

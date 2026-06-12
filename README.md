# metadataswiss-connector

Connector zur Extraktion von Metadaten aus Datenkatalogen, Transformation in das [DCAT-AP-CH](https://www.i14y.admin.ch/)-Format und Publikation auf die [Interoperabilitätsplattform I14Y](https://www.i14y.admin.ch/) des Bundes.

## Architektur

Die Pipeline folgt einem ELT-Pattern mit drei Schritten, jeweils basierend auf [dlt](https://dlthub.com/):

1. **Extract + Load** (`extract`): Rohdaten werden via dlt aus dem Quellkatalog extrahiert und in eine lokale DuckDB geladen.
2. **Transform** (`transform`): Die Rohdaten werden in das I14Y-kompatible DCAT-Format transformiert und in ein separates Dataset geschrieben.
3. **Publish** (`publish`): Die transformierten Records werden gegen die I14Y Partner API synchronisiert (create/update/delete), mit einer per-Source State-Datei zur Nachverfolgung der remote IDs.

```
Quellkatalog → [extract] → [transform] → [publish] → I14Y API
```

Die Schritte können sowohl über die mitgelieferte CLI als auch über [Dagster](https://dagster.io/) ausgeführt werden (siehe Abschnitt [Dagster](#dagster) weiter unten). `registry.py`, `config.py`, `sources/` und `dcat/` bleiben framework-frei — weitere Orchestratoren (Airflow o.ä.) sind dadurch problemlos anbindbar.

### Plugin-Architektur

Katalogquellen werden als framework-freie `CatalogSource`-Descriptoren registriert. Dagster iteriert über die Liste in `sources/__init__.py` und materialisiert pro Source die gewünschten Assets.

### Projektstruktur

```
src/metadataswiss_connector/
├── registry.py                    # CatalogSource, ResourceSpec, TransformFn
├── config.py                      # I14YConfig
├── resources.py                   # dlt-Pipeline-Helper + I14Y-Client-Factory
├── sync.py                        # I14Y Create/Update/Delete Reconciliation
│                                  #   (sync/purge mit kind-Dispatch)
├── sources/
│   ├── __init__.py                # SOURCES — Liste aller registrierten Quellen
│   └── dataspot/
│       ├── __init__.py            # dataspot_catalog_source (CatalogSource)
│       ├── auth.py                # Azure AD OAuth2 + Dataspot Access Key
│       ├── source.py              # REST API Source Definition (dlt)
│       ├── transform.py           # Feld-Mapping Dataspot → DCAT / Concept
│       ├── mappings.py            # Kontrollierte Vokabulare (Themen, Frequenz, …)
│       ├── enrichment.py          # Staatskalender-Lookup, Data-Owner-Resolver
│       ├── structure.py           # SHACL-Turtle-Generator für Dataset-Struktur
│       └── constants.py           # Konstanten + TRANSFORM_VERSION
├── pipeline.py                   # Framework-freie extract/transform/publish-Helper
├── dagster_defs/                 # Dagster-Entrypoint (Assets + Jobs pro Source)
│   ├── __init__.py               #   exportiert `defs: Definitions`
│   ├── assets.py                 #   Asset-Factory pro CatalogSource
│   ├── email_alerts.py           #   Run-Failure-Sensor (Email)
│   └── purge_jobs.py             #   Purge-Job-Factory pro CatalogSource
└── dcat/
    ├── builders.py                # I14Y DCAT-Modell-Builder
    ├── transforms.py              # Raw→DCAT Transform-Pipeline-Schritt
    └── i14y_models.py             # Generierte Pydantic-Modelle aus I14Y OpenAPI-Spec

packages/i14y-client/              # Eigenständiger Partner-API-Client (separates Paket)
```

### Neue Quelle anbinden

1. **Paket anlegen** unter `sources/<name>/` mit:
   - `source.py` — dlt source factory (Zero-Arg Callable, das einen `DltSource` zurückgibt)
   - `transform.py` — Pro Resource eine Funktion mit Signatur
     `transform_fn(record: dict, children: dict[str, list], *, lookups: Lookups, publisher: str) -> BaseModel | tuple[BaseModel, dict]`,
     die einen Raw-Record auf das passende I14Y-Input-Modell abbildet
     (`DcatDatasetInputModel` für Datasets, `CodeListConceptInput` für Concepts).
     Optionale Sidecar-Payloads (z.B. Code-List-Entries, SHACL-Strukturen) als
     zweites Tuple-Element zurückgeben — sync ruft sie nach Create/Update als
     Follow-up via `apply_extras` auf.
   - `__init__.py` — `CatalogSource(name=..., dlt_source_factory=..., resources={<resource>: ResourceSpec(transform_fn, kind="dataset"|"concept")}, transform_version=...)` instanziieren und exportieren
2. **Registrieren** in `sources/__init__.py`: den `CatalogSource` an `SOURCES` anhängen.

Wird die Transform-Logik einer existierenden Source verändert, sollte `transform_version` erhöht werden. Records mit abweichender persistierter Version werden beim nächsten Sync re-published, auch ohne neues `modified`-Datum aus der Quelle.

## Unterstützte Quellen

- **Dataspot** — Extraktion von Datenprodukten via REST API (z.B. [Datenkatalog Basel-Stadt](https://datenkatalog.bs.ch))

## Voraussetzungen

- Python >= 3.14
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
uv sync
```

### Konfiguration

Sämtliche Konfiguration läuft über `.env` (siehe `.env.example` als Vorlage).
`.env` wird beim Import des Pakets automatisch geladen.

Dataspot-Verbindung. dlt löst diese Werte über das `SOURCES__DATASPOT__*`
Env-Naming auf (doppelter Unterstrich pro TOML-Ebene):

```bash
SOURCES__DATASPOT__BASE_URL=https://datenkatalog.bs.ch
SOURCES__DATASPOT__DATABASE_NAME=prod
# Secrets — nie committen:
SOURCES__DATASPOT__TENANT_ID=
SOURCES__DATASPOT__CLIENT_ID=
SOURCES__DATASPOT__CLIENT_SECRET=
SOURCES__DATASPOT__DATASPOT_ACCESS_KEY=
SOURCES__DATASPOT__EXPOSED_CLIENT_ID=
```

## Pipeline starten

Die Pipeline wird über Dagster orchestriert. Pro registrierter Quelle und Resource werden Assets generiert (`<source>_raw → <source>_<resource>_transformed → <source>_<resource>_published`), so dass sich einzelne Schritte gezielt re-materialisieren lassen und die Lineage in der UI sichtbar ist.

```bash
# Dagster-UI starten (Default: http://localhost:3000)
uv run dagster dev
```

Das Modul `metadataswiss_connector.dagster_defs` wird über `[tool.dagster]` in `pyproject.toml` automatisch von `dagster dev` geladen. Ein `full_sync_schedule` (täglich 03:00, Zeitzone `Europe/Zurich`, konfigurierbar über `CONNECTOR_SYNC_CRON` / `CONNECTOR_SYNC_TIMEZONE`) ist registriert, startet aber **gestoppt** — er muss in der Dagster-UI unter *Automation* bewusst aktiviert werden.

## Deployment (Docker Compose)

Für den Produktivbetrieb liegt ein Compose-Setup mit vier Services bei: `postgres` (Dagster-Storage), `connector_code` (gRPC-Code-Server — **hier laufen alle Runs**, deshalb hängt das persistente Daten-Volume an diesem Container), `webserver` (UI auf Port 3000) und `daemon` (Schedules, Run-Queue, Sensoren).

```bash
# 1. .env mit den PRODUKTIV-Credentials befüllen (siehe .env.example)
cp .env.example .env && $EDITOR .env

# 2. Bauen und starten
docker compose up -d --build

# 3. UI öffnen
open http://localhost:3000
```

Persistenz:
- **Dagster-Metadaten** liegen im benannten Volume `postgres_data`.
- **Connector-State** (DuckDB-Warehouse + I14Y-State-Files) liegt per **Bind Mount** auf dem Host — der Container nutzt damit dieselben Dateien wie ein lokales `dagster dev`. Die Host-Pfade sind über `.env` konfigurierbar:
  - `HOST_STATE_DIR` (Default `./data`) → im Container `/mnt/state` (`CONNECTOR_DATA_DIR`)
  - `HOST_DUCKDB_DIR` (Default `.`, Repo-Root) + `DUCKDB_FILENAME` (Default `metadata.duckdb`) → im Container `/mnt/duckdb/<file>` (`DUCKDB_PATH`)

Die App-Secrets kommen aus `.env`; Postgres-Credentials lassen sich über `DAGSTER_PG_*` überschreiben.

> **Umgebungswechsel (Abnahme → Prod):** Die State-Files mappen Source-IDs auf I14Y-UUIDs einer *bestimmten* Umgebung. Beim Wechsel `HOST_STATE_DIR` (und ggf. `HOST_DUCKDB_DIR`) auf ein **leeres, prod-eigenes** Verzeichnis zeigen lassen — niemals die Abnahme-State-Files wiederverwenden, sonst versucht der Sync, in Prod nicht existierende UUIDs zu aktualisieren/löschen.

Nach dem ersten Start: Sensoren (`email_on_run_failure`, `email_on_invalid_records`) und ggf. `full_sync_schedule` in der UI aktivieren. Für den ersten Prod-Lauf empfiehlt sich ein manueller Materialize des `*_published`-Assets statt direkt den Schedule scharf zu schalten.

#### Email-Alerts bei Run-Failures

Ein `run_failure_sensor` (`email_on_run_failure` in `dagster_defs/email_alerts.py`) verschickt bei jedem fehlgeschlagenen Dagster-Run eine Email an die in `EMAIL_TO` hinterlegten Empfänger. Der Body enthält Run-ID, Job-Name und Fehlertext; das vollständige Event-Log wird als `run_<id>.log` angehängt.

SMTP-Konfiguration via `.env` (siehe `.env.example`):

```
SMTP_HOST=smtp.example.ch
SMTP_PORT=587
SMTP_USER=alerts@example.ch
SMTP_PASSWORD=...
SMTP_USE_TLS=true
SMTP_FROM=alerts@example.ch
EMAIL_TO=ops@example.ch,team@example.ch
```

Fehlen `SMTP_HOST`, `SMTP_FROM` oder `EMAIL_TO`, wird der Sensor still übersprungen — der Sensor muss in der Dagster-UI unter *Sensors* aktiviert werden, damit er feuert.

### Purge

Alle aktiven Records einer Source aus I14Y entfernen (Soft-Delete im State-File). Pro registrierter Source existiert ein Dagster-Job `<source>_purge` (z.B. `dataspot_purge`), der über die Dagster-UI unter *Jobs* gestartet wird. Default-Config ist Dry-Run; für eine echte Löschung im Launchpad:

```yaml
ops:
  dataspot_purge_op:
    config:
      dry_run: false
      confirm: DELETE
```

Ohne `confirm: DELETE` bei `dry_run: false` schlägt der Run absichtlich fehl. Resultate (`deleted`, `failed`, `per_resource`) erscheinen als Op-Metadata im Run-Log.

### Ergebnisse

Die Ergebnisse liegen in `dataspot.duckdb`:
- Dataset `<source>_raw` — Rohdaten aus der jeweiligen Quelle (eine Tabelle pro dlt-Resource)
- Dataset `i14y_dcat` — Transformierte Daten im DCAT-Format (alle Quellen, eine Tabelle pro Resource)

Remote-Zustand wird pro (Source, Resource) in `data/<source>_<resource>_ids.json` persistiert (Mapping Source-ID → I14Y-UUID inkl. `modified`-Timestamp und `transform_version`).

## I14Y-Modelle regenerieren

Die Pydantic-Modelle in `src/metadataswiss_connector/dcat/i14y_models.py` werden automatisch aus der I14Y OpenAPI-Spezifikation (`docs/i14y_rest_api.json`) generiert. Bei einer Aktualisierung der Spec:

```bash
uv run datamodel-codegen \
  --input docs/i14y_rest_api.json \
  --input-file-type openapi \
  --output src/metadataswiss_connector/dcat/i14y_models.py \
  --output-model-type pydantic_v2.BaseModel \
  --snake-case-field \
  --use-field-description \
  --field-constraints \
  --target-python-version 3.14 \
  --use-standard-collections \
  --use-union-operator \
  --allow-population-by-field-name
```

Das File ist generiert — nicht von Hand bearbeiten. Kontrakt-Verstöße in `builders.py` oder `transform_to_dataset` werden nach der Regeneration vom Type-Checker bzw. zur Laufzeit von Pydantic gemeldet.

## Tests

Die Unit-Tests decken die reine Transformations-Schicht ab (Dataspot-Record → I14Y-Modell): `dcat/builders.py`, `sources/dataspot/mappings.py` und `sources/dataspot/transform.py`. Diese Funktionen sind ohne Netz/IO testbar, daher muss Dataspot **nicht gemockt** werden — die Eingaben stammen aus versionierten JSON-Fixtures unter `tests/fixtures/`.

```bash
uv run pytest
```

Die Extraktions-Schicht (`source.py`/`auth.py`, dlt-REST-API gegen Dataspot) ist bewusst nicht abgedeckt: Ein Mock der gesamten API testet eher dlt als unseren Code und ist wartungsintensiver. Neue Fachlogik in der Transformation sollte mit einem Fixture-basierten Testfall ergänzt werden.

## Lizenz

Siehe [LICENSE](LICENSE).

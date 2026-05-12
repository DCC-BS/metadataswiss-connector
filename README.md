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

Aktuell gibt es keinen Orchestrator — Schritte werden über die mitgelieferte CLI ausgeführt. Eine Anbindung an Airflow/Dagster/o.ä. ist später möglich, da `registry.py`, `config.py`, `sources/` und `dcat/` framework-frei bleiben.

### Plugin-Architektur

Katalogquellen werden als framework-freie `CatalogSource`-Descriptoren registriert. Die CLI iteriert über die Liste in `sources/__init__.py` und führt pro Source die gewünschten Schritte aus.

### Projektstruktur

```
src/metadataswiss_connector/
├── __main__.py                    # CLI-Entrypoint (list, run, purge)
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
     `transform_fn(record: dict, children: dict[str, list], *, lookups: dict[str, list[dict]], publisher: str) -> BaseModel | tuple[BaseModel, dict]`,
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

Nicht-geheime Einstellungen in `.dlt/config.toml`:

```toml
[sources.dataspot]
base_url = "https://datenkatalog.bs.ch"
database_name = "prod"
```

Secrets in `.dlt/secrets.toml` (nicht committen!):

```toml
[sources.dataspot]
tenant_id = ""
client_id = ""
client_secret = ""
dataspot_access_key = ""
exposed_client_id = ""
```

## Pipeline starten

```bash
# alle registrierten Quellen anzeigen
uv run metadataswiss-connector list

# eine Quelle end-to-end ausführen (Default: max. 5 Records pro Resource publishen)
uv run metadataswiss-connector run dataspot

# Publish-Limit anheben (0 = kein Limit)
uv run metadataswiss-connector run dataspot --limit 0

# nur einzelne Schritte
uv run metadataswiss-connector run dataspot --steps extract,transform

# alle Quellen
uv run metadataswiss-connector run --all
```

### Purge

Alle aktiven Records einer Source aus I14Y entfernen (Soft-Delete im State-File):

```bash
# Dry-Run: zeigt nur, was gelöscht würde
uv run metadataswiss-connector purge dataspot --dry-run

# echte Löschung (interaktive Bestätigung erforderlich)
uv run metadataswiss-connector purge dataspot

# ohne Rückfrage (z.B. für Skripte)
uv run metadataswiss-connector purge dataspot --yes
```

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

Das File ist generiert — nicht von Hand bearbeiten. Kontrakt-Verstöße in `builders.py` oder `transform_to_dcat` werden nach der Regeneration vom Type-Checker bzw. zur Laufzeit von Pydantic gemeldet.

## Lizenz

Siehe [LICENSE](LICENSE).

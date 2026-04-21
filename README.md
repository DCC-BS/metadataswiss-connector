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
├── __main__.py                    # CLI-Entrypoint (list, run)
├── registry.py                    # CatalogSource, TransformFn, run_source
├── config.py                      # I14YConfig
├── resources.py                   # dlt-Pipeline-Helper + I14Y-Client-Factory
├── sync.py                        # I14Y Create/Update/Delete Reconciliation
├── sources/
│   ├── __init__.py                # SOURCES — Liste aller registrierten Quellen
│   └── dataspot/
│       ├── __init__.py            # dataspot_catalog_source (CatalogSource)
│       ├── auth.py                # Azure AD OAuth2 + Dataspot Access Key
│       ├── source.py              # REST API Source Definition (dlt)
│       └── transform.py           # Feld-Mapping Dataspot → DCAT
└── dcat/
    ├── builders.py                # I14Y DCAT-Modell-Builder
    ├── transforms.py              # Raw→DCAT Transform-Pipeline-Schritt
    └── i14y_models.py             # Generierte Pydantic-Modelle aus I14Y OpenAPI-Spec
```

### Neue Quelle anbinden

1. **Paket anlegen** unter `sources/<name>/` mit:
   - `source.py` — dlt source factory (Zero-Arg Callable, das einen `DltSource` zurückgibt)
   - `transform.py` — `transform_fn(record: dict, children: dict[str, list], *, publisher: str) -> BaseModel`, die einen Raw-Record in ein `DcatDatasetInputModel` abbildet
   - `__init__.py` — `CatalogSource(name=..., dlt_source_factory=..., resources={<resource>: <transform_fn>})` instanziieren und exportieren
2. **Registrieren** in `sources/__init__.py`: den `CatalogSource` an `SOURCES` anhängen.

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

# eine Quelle end-to-end ausführen
uv run metadataswiss-connector run dataspot

# nur einzelne Schritte
uv run metadataswiss-connector run dataspot --steps extract,transform

# alle Quellen
uv run metadataswiss-connector run --all
```

### Ergebnisse

Die Ergebnisse liegen in `dataspot.duckdb`:
- Dataset `<source>_raw` — Rohdaten aus der jeweiligen Quelle (eine Tabelle pro dlt-Resource)
- Dataset `i14y_dcat` — Transformierte Daten im DCAT-Format (alle Quellen)

Remote-Zustand wird pro Source in `data/<source>_dataset_ids.json` persistiert.

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

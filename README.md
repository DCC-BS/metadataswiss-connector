# metadataswiss-connector

Connector zur Extraktion von Metadaten aus Datenkatalogen, Transformation in das [DCAT-AP-CH](https://www.i14y.admin.ch/)-Format und Publikation auf die [Interoperabilitätsplattform I14Y](https://www.i14y.admin.ch/) des Bundes.

## Architektur

Die Pipeline folgt einem ELT-Pattern mit zwei Schritten, orchestriert durch [Dagster](https://dagster.io/) mit der offiziellen [`dagster-dlt`](https://docs.dagster.io/integrations/libraries/dlt)-Integration:

1. **Extract + Load** (`@dlt_assets`): Rohdaten werden via [dlt](https://dlthub.com/) aus dem Quellkatalog extrahiert und in eine lokale DuckDB-Datenbank geladen. Jede dlt-Resource (z.B. `data_products`) wird automatisch zu einem eigenen Dagster-Asset mit Row-Counts und Load-Info.
2. **Transform** (`dataspot_i14y_dcat`): Die Rohdaten werden in das I14Y-kompatible DCAT-Format transformiert und in ein separates Dataset geschrieben.

Beide Schritte sind als Dagster-Assets modelliert und liegen in derselben DuckDB-Datei, was die Nachvollziehbarkeit gewährleistet.

```
Quellkatalog → [dataspot/data_products] → [dataspot_i14y_dcat] → (I14Y API)
```

### Projektstruktur

```
src/metadataswiss_connector/
├── definitions.py                 # Dagster Definitions Einstiegspunkt
├── resources.py                   # Geteilte dlt-Pipeline-/Destination-Factories
├── assets/
│   ├── dataspot.py                # @dlt_assets + DataspotDltTranslator
│   └── dcat.py                    # Downstream DCAT-Transform-Asset
├── sources/
│   └── dataspot/                  # Dataspot-spezifisch
│       ├── auth.py                # Azure AD OAuth2 + Dataspot Access Key
│       ├── source.py              # REST API Source Definition (dlt)
│       └── transform.py           # Feld-Mapping Dataspot → DCAT
└── dcat/
    ├── builders.py                # Shared: I14Y DCAT-Modell-Builder (MultiLanguageModel, AgentModel, etc.)
    └── transforms.py              # Shared: Raw→DCAT Transform-Pipeline-Schritt
```

Um einen neuen Katalog anzubinden, wird ein neues Paket unter `sources/` erstellt (z.B. `sources/ckan/`) mit eigener Source-Definition und Feld-Mapping, plus ein zugehöriges Asset-Modul unter `assets/`. Die DCAT-Builder (`dcat/builders.py`) und die Transform-Logik (`dcat/transforms.py`) werden geteilt.

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
exposed_client_id = ""
```

Secrets in `.dlt/secrets.toml` (nicht committen!):

```toml
[sources.dataspot]
tenant_id = ""
client_id = ""
client_secret = ""
dataspot_access_key = ""
```

## Pipeline starten

```bash
uv run dagster dev
```

Öffnet die Dagster-Weboberfläche unter [http://127.0.0.1:3000](http://127.0.0.1:3000). Dort können die Assets `dataspot/data_products` und `dataspot_i14y_dcat` manuell materialisiert oder als Job ausgeführt werden.

### Ergebnisse

Die Ergebnisse liegen in `dataspot.duckdb`:
- Dataset `dataspot_raw` — Rohdaten aus Dataspot (eine Tabelle pro dlt-Resource)
- Dataset `i14y_dcat` — Transformierte Daten im DCAT-Format

## Lizenz

Siehe [LICENSE](LICENSE).

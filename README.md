# metadataswiss-connector

Connector zur Extraktion von Metadaten aus Datenkatalogen und Publikation auf die [Interoperabilitätsplattform I14Y](https://www.i14y.admin.ch/) des Bundes.

## Unterstützte Quellen

- **Dataspot** — Extraktion von Datenprodukten via REST API

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
uv run python -m src.dataspot.pipeline
```

## Lizenz

Siehe [LICENSE](LICENSE).

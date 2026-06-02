"""metadataswiss-connector package.

Loads ``.env`` on import so every entrypoint — Dagster, the framework-free
``pipeline`` helpers, and the debug scripts — sees the same configuration.
Both the connector's own ``os.environ`` reads (e.g. ``I14YConfig`` and the
Dataspot constants) and dlt's ``SOURCES__DATASPOT__*`` config/secrets
resolution rely on these variables being present in the environment.

``load_dotenv`` never overrides variables already set in the real
environment, so container/CI injection still takes precedence over ``.env``.
"""

from dotenv import load_dotenv

load_dotenv()

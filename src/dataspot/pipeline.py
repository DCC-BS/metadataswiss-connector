"""Run the Dataspot extraction pipeline."""

import dlt

from .source import dataspot_source


def run():
    pipeline = dlt.pipeline(
        pipeline_name="dataspot",
        destination="duckdb",
        dataset_name="dataspot_raw",
    )

    config = dlt.config
    secrets = dlt.secrets

    source = dataspot_source(
        base_url=config["sources.dataspot.base_url"],
        database_name=config["sources.dataspot.database_name"],
        exposed_client_id=config["sources.dataspot.exposed_client_id"],
        tenant_id=secrets["sources.dataspot.tenant_id"],
        client_id=secrets["sources.dataspot.client_id"],
        client_secret=secrets["sources.dataspot.client_secret"],
        dataspot_access_key=secrets["sources.dataspot.dataspot_access_key"],
    )

    load_info = pipeline.run(source)
    print(load_info)


if __name__ == "__main__":
    run()

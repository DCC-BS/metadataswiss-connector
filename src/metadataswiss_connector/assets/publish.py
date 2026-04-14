"""Assets that sync transformed DCAT data to the I14Y Partner API."""

from pathlib import Path

from dagster import AssetExecutionContext, asset

from metadataswiss_connector.dcat.transforms import read_transformed
from metadataswiss_connector.resources import I14YResource
from metadataswiss_connector.sync import sync_datasets

# Limit how many source records to sync per run (set to None for all).
DEFAULT_SYNC_LIMIT = 5

STATE_PATH = Path("data/dataset_ids.json")


@asset(
    group_name="i14y_publish",
    deps=["dataspot_i14y_dcat"],
    required_resource_keys={"i14y"},
)
def sync_datasets_to_i14y(context: AssetExecutionContext) -> None:
    """Sync transformed DCAT datasets to the I14Y environment.

    Compares the local transformed records against the persisted state
    file and creates, updates, or deletes datasets as needed.
    """
    i14y_resource: I14YResource = context.resources.i14y
    records = read_transformed("data_products", limit=DEFAULT_SYNC_LIMIT)

    if not records:
        context.log.warning("No transformed records found to sync")
        return

    context.log.info("Starting sync with %d source records", len(records))

    with i14y_resource.get_client() as client:
        result = sync_datasets(client, records, state_path=STATE_PATH)

    context.log.info("Sync complete: %s", result.summary())

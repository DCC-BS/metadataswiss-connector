# i14y-client

Python client for the [I14Y Partner API](https://www.i14y.admin.ch/) (Swiss Interoperability Platform).

## Installation

```bash
pip install i14y-client
```

## Quick start

```python
from i14y_client import I14YAuth, I14YClient

auth = I14YAuth(
    token_url="https://identity.i14y-a.c.bfs.admin.ch/realms/bfs-sis-a/protocol/openid-connect/token",
    client_id="your-client-id",
    client_secret="your-client-secret",
)

with I14YClient(
    base_url="https://api-a.i14y.admin.ch/api/partner/v1",
    auth=auth,
    user_agent="MyApp/1.0 (My Organisation; contact: team@example.org)",
) as client:
    # List datasets
    datasets = client.datasets.list()

    # Create a dataset
    from i14y_client.models import DcatDatasetInputModel, MultiLanguageModel, CodeInputModel, IdentifierInputModel

    dataset = DcatDatasetInputModel(
        title=MultiLanguageModel(de="Mein Datensatz"),
        description=MultiLanguageModel(de="Beschreibung"),
        publisher=IdentifierInputModel(identifier="my-org"),
        access_rights=CodeInputModel(code="PUBLIC"),
    )
    dataset_id = client.datasets.create(dataset)
```

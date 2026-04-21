"""I14Y Partner API client."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from uuid import UUID

import httpx

from i14y_client.auth import I14YAuth
from i14y_client.models import (
    DcatDatasetInputModel,
    DcatDatasetInputModelDataWrapper,
    DcatDatasetModel,
    DcatDatasetModelCollectionDataWrapper,
    DcatDatasetModelDataWrapper,
)

logger = logging.getLogger(__name__)

# Default I14Y environments
I14Y_PROD = "https://api.i14y.admin.ch/api/partner/v1"
I14Y_ABN = "https://api-a.i14y.admin.ch/api/partner/v1"


class I14YError(Exception):
    """Raised when the I14Y API returns an error response."""

    def __init__(self, status_code: int, detail: str, response: httpx.Response) -> None:
        self.status_code = status_code
        self.detail = detail
        self.response = response
        super().__init__(f"I14Y API error {status_code}: {detail}")


@dataclass
class I14YClient:
    """Synchronous client for the I14Y Partner API.

    Usage::

        from i14y_client import I14YAuth, I14YClient

        auth = I14YAuth(
            token_url="https://identity.i14y-a.c.bfs.admin.ch/...",
            client_id="...",
            client_secret="...",
        )
        client = I14YClient(
            base_url=I14YClient.ABN,
            auth=auth,
            user_agent="MyApp/1.0 (My Org; contact: team@example.org)",
        )

        # Create a dataset
        dataset_id = client.datasets.create(dataset_input)

        # Get a dataset
        dataset = client.datasets.get(dataset_id)
    """

    base_url: str
    auth: I14YAuth
    user_agent: str
    _http: httpx.Client = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        self._http = httpx.Client(
            base_url=self.base_url,
            auth=self.auth,
            headers={
                "Content-Type": "application/json",
                "User-Agent": self.user_agent,
            },
            timeout=30.0,
        )
        self.datasets = DatasetResource(self)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> I14YClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        params: dict | None = None,
    ) -> httpx.Response:
        response = self._http.request(method, path, json=json, params=params)
        if response.status_code >= 400:
            try:
                detail = response.json()
            except Exception:
                detail = response.text
            raise I14YError(response.status_code, str(detail), response)
        return response


class DatasetResource:
    """Operations on DCAT datasets."""

    def __init__(self, client: I14YClient) -> None:
        self._client = client

    def create(self, dataset: DcatDatasetInputModel) -> UUID:
        """Create a new dataset. Returns the assigned UUID."""
        wrapper = DcatDatasetInputModelDataWrapper(data=dataset)
        body = wrapper.model_dump(by_alias=True, exclude_none=True, mode="json")
        response = self._client._request("POST", "/datasets", json=body)
        return UUID(response.json())

    def get(self, dataset_id: UUID | str) -> DcatDatasetModel:
        """Retrieve a dataset by ID."""
        response = self._client._request("GET", f"/datasets/{dataset_id}")
        wrapper = DcatDatasetModelDataWrapper.model_validate(response.json())
        return wrapper.data

    def update(self, dataset_id: UUID | str, dataset: DcatDatasetInputModel) -> None:
        """Update an existing dataset."""
        wrapper = DcatDatasetInputModelDataWrapper(data=dataset)
        body = wrapper.model_dump(by_alias=True, exclude_none=True, mode="json")
        self._client._request("PUT", f"/datasets/{dataset_id}", json=body)

    def delete(self, dataset_id: UUID | str) -> None:
        """Delete a dataset."""
        self._client._request("DELETE", f"/datasets/{dataset_id}")

    def set_publication_level(self, dataset_id: UUID | str, level: str) -> None:
        """Set the publication level (e.g. 'Public', 'Internal')."""
        self._client._request(
            "PUT", f"/datasets/{dataset_id}/publication-level",
            params={"level": level},
        )

    def set_registration_status(self, dataset_id: UUID | str, status: str) -> None:
        """Set the registration status (e.g. 'Recorded', 'Candidate')."""
        self._client._request(
            "PUT", f"/datasets/{dataset_id}/registration-status",
            params={"status": status},
        )

    def list(
        self,
        *,
        dataset_identifier: str | None = None,
        publisher_identifier: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> list[DcatDatasetModel]:
        """List datasets with optional filters."""
        params = {"page": page, "pageSize": page_size}
        if dataset_identifier:
            params["datasetIdentifier"] = dataset_identifier
        if publisher_identifier:
            params["publisherIdentifier"] = publisher_identifier
        response = self._client._request("GET", "/datasets", params=params)
        wrapper = DcatDatasetModelCollectionDataWrapper.model_validate(response.json())
        return wrapper.data or []

    def list_all(
        self,
        *,
        publisher_identifier: str | None = None,
        page_size: int = 100,
    ) -> list[DcatDatasetModel]:
        """Fetch all datasets by paginating through all pages."""
        all_datasets: list[DcatDatasetModel] = []
        page = 1
        while True:
            batch = self.list(
                publisher_identifier=publisher_identifier,
                page=page,
                page_size=page_size,
            )
            if not batch:
                break
            all_datasets.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
        return all_datasets

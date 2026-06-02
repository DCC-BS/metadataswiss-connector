"""I14Y Partner API client."""

from __future__ import annotations

import contextvars
import json as _json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, TypeVar
from uuid import UUID

import httpx
from pydantic import BaseModel

from i14y_client.auth import I14YAuth
from i14y_client.models import (
    CodeListConceptInput,
    ConceptInputBase,
    ConceptInputBaseDataWrapper,
    ConceptType,
    DataServiceInputModel,
    DataServiceInputModelDataWrapper,
    DataServiceModel,
    DataServiceModelCollectionDataWrapper,
    DataServiceModelDataWrapper,
    DateConceptInput,
    DcatDatasetInputModel,
    DcatDatasetInputModelDataWrapper,
    DcatDatasetModel,
    DcatDatasetModelCollectionDataWrapper,
    DcatDatasetModelDataWrapper,
    IopConceptModel,
    IopConceptModelCollectionDataWrapper,
    IopConceptModelDataWrapper,
    NumericConceptInput,
    StringConceptInput,
)

# The Partner API uses ``conceptType`` as the polymorphic discriminator
# on ``data`` but the typed input subclasses don't carry it as a field.
# We tag the body explicitly at serialization time.
_CONCEPT_TYPE_BY_INPUT: dict[type, ConceptType] = {
    CodeListConceptInput: ConceptType.code_list,
    DateConceptInput: ConceptType.date,
    NumericConceptInput: ConceptType.numeric,
    StringConceptInput: ConceptType.string,
}

logger = logging.getLogger(__name__)

# Default I14Y environments
I14Y_PROD = "https://api.i14y.admin.ch/api/partner/v1"
I14Y_ABN = "https://api-a.i14y.admin.ch/api/partner/v1"

# I14Y publication-level codes (passed to ``set_publication_level``).
PUBLICATION_LEVEL_PUBLIC = "Public"
PUBLICATION_LEVEL_INTERNAL = "Internal"

# I14Y registration-status codes (passed to ``set_registration_status``).
REGISTRATION_STATUS_RECORDED = "Recorded"

# I14Y propagates publication-level changes asynchronously; setting
# registration status or deleting too quickly afterwards yields a 409.
STATUS_PROPAGATION_DELAY_S = 0.5


# Caller-tagged fields (e.g. the upstream source identifier) that the
# request logger appends to every line, so a failing HTTP call can be
# traced back to the originating dataspot record without threading the
# id through every method signature.
_LOG_CONTEXT: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar(
    "i14y_log_context", default={},
)


@contextmanager
def log_context(**fields: str) -> Iterator[None]:
    """Tag every I14Y request log emitted inside the block with ``fields``.

    Used by the sync layer to attach the source identifier (e.g. the
    dataspot UUID) so a failing POST/PUT/DELETE is traceable back to the
    upstream record from the log line alone.
    """
    merged = {**_LOG_CONTEXT.get(), **{k: str(v) for k, v in fields.items() if v is not None}}
    token = _LOG_CONTEXT.set(merged)
    try:
        yield
    finally:
        _LOG_CONTEXT.reset(token)


def _format_log_context() -> str:
    ctx = _LOG_CONTEXT.get()
    if not ctx:
        return ""
    return " " + " ".join(f"{k}={v}" for k, v in ctx.items())


def _estimate_request_size(*, json: dict | None, files: dict | None) -> int:
    """Best-effort byte count for the outgoing payload.

    Used purely for telemetry — we don't reserialize precisely the way
    httpx will, just enough to give an order-of-magnitude. Multipart
    uploads only count the file part, not the form envelope.
    """
    if files is not None:
        total = 0
        for value in files.values():
            # httpx accepts ``(filename, content, content_type)`` tuples
            # or a bare content value; we use the tuple form everywhere.
            content = value[1] if isinstance(value, tuple) and len(value) >= 2 else value
            if isinstance(content, (bytes, bytearray)):
                total += len(content)
            elif isinstance(content, str):
                total += len(content.encode("utf-8"))
        return total
    if json is not None:
        return len(_json.dumps(json).encode("utf-8"))
    return 0


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
    # Logger is overridable so callers (e.g. a Dagster asset) can pass
    # their own logger and have request/response telemetry land next to
    # the materialization in the run log.
    logger: logging.Logger = field(
        default_factory=lambda: logging.getLogger("i14y_client")
    )
    _http: httpx.Client = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        # No default Content-Type: httpx infers it from the body kind
        # (``json=`` → application/json, ``files=`` → multipart/...).
        # That keeps the JSON path and the multipart import_entries path
        # in the same _request method without header conflicts.
        self._http = httpx.Client(
            base_url=self.base_url,
            auth=self.auth,
            headers={"User-Agent": self.user_agent},
            timeout=30.0,
        )
        self.datasets = DatasetResource(self)
        self.concepts = ConceptResource(self)
        self.dataservices = DataServiceResource(self)

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
        files: dict | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        kwargs: dict = {"params": params}
        if files is not None:
            kwargs["files"] = files
        elif json is not None:
            kwargs["json"] = json
        if timeout is not None:
            kwargs["timeout"] = timeout

        req_bytes = _estimate_request_size(json=json, files=files)
        start = time.perf_counter()
        response = self._http.request(method, path, **kwargs)
        duration_ms = (time.perf_counter() - start) * 1000.0
        # I14Y returns the correlation id under one of these headers
        # depending on the gateway; check both before giving up.
        request_id = (
            response.headers.get("x-request-id")
            or response.headers.get("request-id")
            or response.headers.get("x-correlation-id")
        )

        if response.status_code >= 400:
            try:
                detail = response.json()
            except Exception:
                detail = response.text
            self.logger.error(
                "I14Y %s %s -> %d in %.0fms (req=%dB resp=%dB request_id=%s)%s body=%s",
                method, path, response.status_code, duration_ms,
                req_bytes, len(response.content), request_id,
                _format_log_context(), detail,
            )
            raise I14YError(response.status_code, str(detail), response)

        self.logger.info(
            "I14Y %s %s -> %d in %.0fms (req=%dB resp=%dB request_id=%s)%s",
            method, path, response.status_code, duration_ms,
            req_bytes, len(response.content), request_id,
            _format_log_context(),
        )
        return response


TInput = TypeVar("TInput", bound=BaseModel)
TOutput = TypeVar("TOutput", bound=BaseModel)


class _BaseResource(Generic[TInput, TOutput]):
    """Shared CRUD + lifecycle plumbing for I14Y resources.

    Subclasses bind the resource path and pydantic wrappers via class
    attributes. The ``_serialize_input`` hook lets a subclass tweak the
    request body (e.g. concepts inject the ``conceptType`` discriminator).
    """

    path: ClassVar[str]
    list_filter_param: ClassVar[str]
    _input_wrapper: ClassVar[type[BaseModel]]
    _output_wrapper: ClassVar[type[BaseModel]]
    _collection_wrapper: ClassVar[type[BaseModel]]

    def __init__(self, client: I14YClient) -> None:
        self._client = client

    def _serialize_input(self, model: TInput) -> dict:
        wrapper = self._input_wrapper(data=model)
        return wrapper.model_dump(by_alias=True, exclude_none=True, mode="json")

    def create(self, model: TInput) -> UUID:
        body = self._serialize_input(model)
        response = self._client._request("POST", self.path, json=body)
        return UUID(response.json())

    def get(self, id_: UUID | str) -> TOutput:
        response = self._client._request("GET", f"{self.path}/{id_}")
        wrapper = self._output_wrapper.model_validate(response.json())
        return wrapper.data

    def update(self, id_: UUID | str, model: TInput) -> None:
        body = self._serialize_input(model)
        self._client._request("PUT", f"{self.path}/{id_}", json=body)

    def delete(self, id_: UUID | str) -> None:
        self._client._request("DELETE", f"{self.path}/{id_}")

    def set_publication_level(self, id_: UUID | str, level: str) -> None:
        self._client._request(
            "PUT", f"{self.path}/{id_}/publication-level",
            params={"level": level},
        )

    def set_registration_status(self, id_: UUID | str, status: str) -> None:
        self._client._request(
            "PUT", f"{self.path}/{id_}/registration-status",
            params={"status": status},
        )

    def publish_initial(self, id_: UUID | str) -> None:
        """Promote a freshly created record to ``Public`` + ``Recorded``.

        I14Y propagates the publication-level change asynchronously, so
        we wait briefly before flipping the registration status or the
        second call returns 409.
        """
        self.set_publication_level(id_, PUBLICATION_LEVEL_PUBLIC)
        time.sleep(STATUS_PROPAGATION_DELAY_S)
        self.set_registration_status(id_, REGISTRATION_STATUS_RECORDED)

    def decommission_and_delete(self, id_: UUID | str) -> None:
        """Demote to ``Internal`` and delete.

        I14Y refuses to delete a ``Public`` record, so we demote first.
        Same async propagation caveat as ``publish_initial``.
        """
        self.set_publication_level(id_, PUBLICATION_LEVEL_INTERNAL)
        time.sleep(STATUS_PROPAGATION_DELAY_S)
        self.delete(id_)

    def apply_extras(self, id_: UUID | str, extras: dict) -> None:
        """No follow-up payload by default; subclasses override."""
        return None

    def list(
        self,
        *,
        identifier: str | None = None,
        publisher_identifier: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> list[TOutput]:
        params: dict = {"page": page, "pageSize": page_size}
        if identifier:
            params[self.list_filter_param] = identifier
        if publisher_identifier:
            params["publisherIdentifier"] = publisher_identifier
        response = self._client._request("GET", self.path, params=params)
        wrapper = self._collection_wrapper.model_validate(response.json())
        return wrapper.data or []

    def list_all(
        self,
        *,
        publisher_identifier: str | None = None,
        page_size: int = 100,
    ) -> list[TOutput]:
        out: list[TOutput] = []
        page = 1
        while True:
            batch = self.list(
                publisher_identifier=publisher_identifier,
                page=page,
                page_size=page_size,
            )
            if not batch:
                break
            out.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
        return out


class DatasetResource(_BaseResource[DcatDatasetInputModel, DcatDatasetModel]):
    """Operations on DCAT datasets."""

    path = "/datasets"
    list_filter_param = "datasetIdentifier"
    _input_wrapper = DcatDatasetInputModelDataWrapper
    _output_wrapper = DcatDatasetModelDataWrapper
    _collection_wrapper = DcatDatasetModelCollectionDataWrapper

    def delete_structure(self, dataset_id: UUID | str) -> None:
        """Drop any existing SHACL structure on the dataset.

        I14Y's import endpoint refuses to overwrite a populated structure,
        so we always DELETE first to make the import a true replace. The
        endpoint is a no-op (404) on datasets without a structure yet.
        """
        try:
            self._client._request("DELETE", f"/datasets/{dataset_id}/structures")
        except I14YError as exc:
            if exc.status_code != 404:
                raise

    def import_structure(
        self,
        dataset_id: UUID | str,
        turtle: str,
        *,
        filename: str = "structure.ttl",
    ) -> None:
        """Upload a SHACL Turtle structure for the dataset (replace).

        Always deletes first so a dataset that lost an attribute upstream
        loses it on I14Y too — the import endpoint by itself rejects an
        already-populated structure.
        """
        self.delete_structure(dataset_id)
        files = {"file": (filename, turtle.encode("utf-8"), "text/turtle")}
        self._client._request(
            "POST",
            f"/datasets/{dataset_id}/structures/imports",
            files=files,
            timeout=60.0,
        )

    def apply_extras(self, dataset_id: UUID | str, extras: dict) -> None:
        """Dispatch transform sidecars to follow-up endpoints.

        Currently routes the ``structure`` sidecar (SHACL Turtle string)
        to the structures-import endpoint.
        """
        structure = extras.get("structure")
        if structure:
            self.import_structure(dataset_id, structure)

    def decommission_and_delete(self, id_: UUID | str) -> None:
        """Drop the SHACL structure before deleting the dataset itself.

        I14Y keeps the structure as a sub-resource of the dataset; leaving
        it behind blocks the DELETE on the parent.
        """
        self.delete_structure(id_)
        super().decommission_and_delete(id_)


class DataServiceResource(_BaseResource[DataServiceInputModel, DataServiceModel]):
    """Operations on DCAT data services (APIs)."""

    path = "/dataservices"
    list_filter_param = "dataServiceIdentifier"
    _input_wrapper = DataServiceInputModelDataWrapper
    _output_wrapper = DataServiceModelDataWrapper
    _collection_wrapper = DataServiceModelCollectionDataWrapper


class ConceptResource(_BaseResource[ConceptInputBase, IopConceptModel]):
    """Operations on concepts.

    Currently only ``CodeListConceptInput`` is exercised on write paths;
    the underlying API also accepts Date/Numeric/String concept inputs.
    """

    path = "/concepts"
    list_filter_param = "conceptIdentifier"
    _input_wrapper = ConceptInputBaseDataWrapper
    _output_wrapper = IopConceptModelDataWrapper
    _collection_wrapper = IopConceptModelCollectionDataWrapper

    def _serialize_input(self, model: ConceptInputBase) -> dict:
        # The Partner API uses ``conceptType`` as the polymorphic
        # discriminator on ``data`` but the typed input subclasses don't
        # carry it as a field. Tag the body explicitly here.
        concept_type = _CONCEPT_TYPE_BY_INPUT.get(type(model))
        if concept_type is None:
            raise TypeError(f"Unsupported concept input type: {type(model).__name__}")
        body = super()._serialize_input(model)
        body["data"]["conceptType"] = concept_type.value
        return body

    def delete_entries(self, concept_id: UUID | str) -> None:
        """Delete all code-list entries for a concept."""
        self._client._request("DELETE", f"/concepts/{concept_id}/codelist-entries")

    def import_entries(
        self, concept_id: UUID | str, entries: list[dict]
    ) -> None:
        """Replace all code-list entries for a concept (bulk JSON import).

        I14Y's ``imports/Json`` endpoint refuses to overwrite an existing
        entry set ("The concept contains entries. Please delete them and
        try again." — 405), so we always DELETE first to make this a
        true replace. The DELETE is a no-op against a concept that has
        no entries yet (fresh concepts).

        Uploads a JSON file via multipart/form-data. CodeListEntryModel
        requires ``conceptId`` per the OpenAPI schema, so we inject it
        from the path argument.
        """
        try:
            self.delete_entries(concept_id)
        except I14YError as exc:
            # 404 just means there were no entries to delete, which is
            # the desired state. Anything else is a real failure.
            if exc.status_code != 404:
                raise
        cid = str(concept_id)
        body = {"data": [{**entry, "conceptId": cid} for entry in entries]}
        files = {"file": ("entries.json", _json.dumps(body), "application/json")}
        self._client._request(
            "POST",
            f"/concepts/{concept_id}/codelist-entries/imports/Json",
            files=files,
            timeout=60.0,
        )

    def apply_extras(self, concept_id: UUID | str, extras: dict) -> None:
        """Dispatch transform sidecars to follow-up endpoints.

        Currently routes the ``entries`` sidecar to the bulk
        code-list-entries import endpoint.
        """
        entries = extras.get("entries")
        if entries:
            self.import_entries(concept_id, entries)

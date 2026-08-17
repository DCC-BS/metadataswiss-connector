"""Gating of I14Y publication via the ``publish_on_i14y`` custom property.

Only records — data products (datasets + data services) *and* code lists
— whose ``publish_on_i14y`` custom property is an explicit ``"yes"`` are
published. The gate runs at extract on ``data_products`` /
``data_services`` / ``code_lists``, whose raw tables use
``write_disposition="replace"`` so a record that loses its ``"yes"``
drops out of the raw table and is decommissioned on the next sync — a
``merge`` table would retain the stale row and keep publishing it.

The broad ``data_products_all`` stream stays ungated (public-filtered
only): it feeds ancestry / contact resolution for every product.

The source-level tests build the dlt source and reach into dlt-internal
pipe structures (``resource._pipe`` / ``FilterItem``) to assert the gate
is wired in. They use dummy credentials: the source is only constructed,
never run, so no network call is made.
"""

import pytest

from metadataswiss_connector.sources.dataspot.source import (
    _is_public,
    _publish_on_i14y,
    dataspot_source,
)


def _item(value) -> dict:
    """Record carrying the given ``publish_on_i14y`` custom-property value."""
    return {"customProperties": {"publish_on_i14y": value}}


class TestPublishOnI14yPredicate:
    def test_yes_publishes(self):
        assert _publish_on_i14y(_item("yes")) is True

    def test_no_does_not_publish(self):
        assert _publish_on_i14y(_item("no")) is False

    def test_missing_property_defaults_to_no(self):
        assert _publish_on_i14y({"customProperties": {}}) is False

    def test_missing_custom_properties_defaults_to_no(self):
        assert _publish_on_i14y({}) is False

    @pytest.mark.parametrize("value", ["YES", "Yes", "yES", " yes ", "yes\n"])
    def test_case_and_whitespace_insensitive(self, value):
        assert _publish_on_i14y(_item(value)) is True

    @pytest.mark.parametrize("value", ["y", "true", "1", "ja", "", "yesish", "no "])
    def test_other_strings_do_not_publish(self, value):
        assert _publish_on_i14y(_item(value)) is False

    @pytest.mark.parametrize("value", [True, 1, None, ["yes"]])
    def test_non_string_values_do_not_publish(self, value):
        # Only the literal string "yes" opts in; a boolean/number/None
        # (e.g. from a differently-typed custom property) never does.
        assert _publish_on_i14y(_item(value)) is False


def _build_source():
    return dataspot_source(
        base_url="https://dataspot.example",
        database_name="db",
        exposed_client_id="client",
        tenant_id="tenant",
        client_id="ci",
        client_secret="cs",
        dataspot_access_key="key",
    )


def _transformer_output(source, name: str, items: list[dict]) -> list[str]:
    """Ids yielded when ``name``'s transformer generator runs over ``items``."""
    return [item["id"] for item in source.resources[name]._pipe.gen(items)]


def _filter_predicates(source, name: str) -> list:
    """The filter predicates wired onto resource ``name`` (dlt internals)."""
    from dlt.extract.items_transform import FilterItem

    return [s._f for s in source.resources[name]._pipe if isinstance(s, FilterItem)]


class TestDataProductGating:
    @pytest.fixture
    def source(self):
        return _build_source()

    @pytest.fixture
    def items(self) -> list[dict]:
        return [
            {"id": "ogd-yes", "stereotype": "OGD", **_item("yes")},
            {"id": "ogd-no", "stereotype": "OGD", **_item("no")},
            {"id": "ogd-missing", "stereotype": "OGD", "customProperties": {}},
            {"id": "geo-yes", "stereotype": "GEO", **_item("yes")},
            {"id": "api-yes", "stereotype": "API", **_item("yes")},
            {"id": "api-no", "stereotype": "API", **_item("no")},
        ]

    def test_data_products_keep_only_opted_in_datasets(self, source, items):
        # OGD/GEO with "yes"; API records and non-opted-in ones drop out.
        assert _transformer_output(source, "data_products", items) == [
            "ogd-yes",
            "geo-yes",
        ]

    def test_data_services_keep_only_opted_in_apis(self, source, items):
        assert _transformer_output(source, "data_services", items) == ["api-yes"]

    def test_data_products_uses_replace_disposition(self, source):
        # replace (not merge) is what makes the gate effective: a merge raw
        # table retains rows no longer yielded, so a dataset that loses its
        # "yes" would keep being published. Replace rebuilds the table as
        # exactly the opted-in set each run.
        assert source.resources["data_products"].write_disposition == "replace"

    def test_data_services_uses_replace_disposition(self, source):
        assert source.resources["data_services"].write_disposition == "replace"


class TestCodeListGating:
    # Code lists surface ``publish_on_i14y`` on the enumeration list item,
    # so they carry the same gate as data products.
    def test_code_lists_gated_by_publish_property(self):
        preds = _filter_predicates(_build_source(), "code_lists")
        assert _is_public in preds
        assert _publish_on_i14y in preds

    def test_code_lists_use_replace_disposition(self):
        # replace (not merge) so a code list that loses its "yes" — or is
        # removed — drops out of the raw table and is decommissioned.
        assert _build_source().resources["code_lists"].write_disposition == "replace"


class TestNotGated:
    def test_broad_product_stream_not_gated(self):
        # ``data_products_all`` feeds ancestry / contact resolution for
        # every product, so it stays public-filtered only, never
        # publish-gated (the gate lives on data_products / data_services).
        preds = _filter_predicates(_build_source(), "data_products_all")
        assert _is_public in preds
        assert _publish_on_i14y not in preds

from openeo_aggregator.metadata.models.stac_summaries import StacSummaries
from openeo_aggregator.testing import approx_str_prefix


def test_merge_eo_bands_empty(reporter):
    summaries_list = []
    result = StacSummaries.merge_all(
        summaries_by_backend={},
        report=reporter,
        collection_id="S2",
    ).to_dict()
    assert result == {}


def test_merge_eo_bands_simple(reporter):
    b02 = {"common_name": "blue", "name": "B02"}
    b03 = {"common_name": "green", "name": "B03"}
    summaries_by_backend = {
        "x": StacSummaries.from_dict({"eo:bands": [b02, b03]}),
        "y": StacSummaries.from_dict({"eo:bands": [b02, b03]}),
    }
    result = StacSummaries.merge_all(
        summaries_by_backend=summaries_by_backend, report=reporter, collection_id="S2"
    ).to_dict()
    assert result == {"eo:bands": [b02, b03]}


def test_merge_eo_bands_empty_prefix(reporter):
    b02 = {"common_name": "blue", "name": "B02"}
    b03 = {"common_name": "green", "name": "B03"}
    summaries_by_backend = {
        "x": StacSummaries.from_dict({"eo:bands": [b02, b03]}),
        "y": StacSummaries.from_dict({"eo:bands": [b03, b02]}),
    }
    result = StacSummaries.merge_all(
        summaries_by_backend=summaries_by_backend, report=reporter, collection_id="S2"
    ).to_dict()
    assert result == {"eo:bands": [b02, b03]}
    assert reporter.logs == [
        {
            "msg": "Empty prefix for 'eo:bands', falling back to first back-end's 'eo:bands'",
            "collection_id": "S2",
        }
    ]


def test_merge_eo_bands_invalid(reporter):
    b02 = {"common_name": "blue", "name": "B02"}
    b03 = {"common_name": "green", "name": "B03"}
    summaries_by_backend = {
        "x": StacSummaries.from_dict({"eo:bands": [{"name": 123455}]}),
        "y": StacSummaries.from_dict({"eo:bands": [b03, b02]}),
    }
    result = StacSummaries.merge_all(
        summaries_by_backend=summaries_by_backend, report=reporter, collection_id="S2"
    ).to_dict()
    assert result == {"eo:bands": [b03, b02]}
    assert reporter.logs == [
        {
            "msg": approx_str_prefix("Failed to parse summary 'eo:bands'"),
            "collection_id": "S2",
        }
    ]

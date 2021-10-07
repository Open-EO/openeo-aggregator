import pytest

from openeo_aggregator.egi import parse_eduperson_entitlement, Entitlement, is_early_adopter


def test_parse_eduperson_entitlement():
    assert parse_eduperson_entitlement(
        "urn:mace:egi.eu:group:vo.openeo.cloud#aai.egi.eu"
    ) == Entitlement(
        namespace="urn:mace:egi.eu", vo="vo.openeo.cloud", group=None, role=None, authority="aai.egi.eu"
    )
    assert parse_eduperson_entitlement(
        "urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu"
    ) == Entitlement(
        namespace="urn:mace:egi.eu", vo="vo.openeo.cloud", group=None, role="early_adopter", authority="aai.egi.eu"
    )
    assert parse_eduperson_entitlement(
        "urn:mace:egi.eu:group:vo.openeo.cloud:vo.openeo-sub.cloud:role=early_adopter#aai.egi.eu"
    ) == Entitlement(
        namespace="urn:mace:egi.eu", vo="vo.openeo.cloud", group="vo.openeo-sub.cloud", role="early_adopter",
        authority="aai.egi.eu"
    )
    assert parse_eduperson_entitlement(
        "urn:mace:egi-dev.eu:group:vo.openeo-dev.cloud:vo.openeo-sub.cloud:role=Early-Adop.ter#aai.egi-dev.eu"
    ) == Entitlement(
        namespace="urn:mace:egi-dev.eu", vo="vo.openeo-dev.cloud", group="vo.openeo-sub.cloud", role="Early-Adop.ter",
        authority="aai.egi-dev.eu"
    )


def test_parse_eduperson_entitlement_strict():
    with pytest.raises(ValueError, match="Failed to parse"):
        parse_eduperson_entitlement("foobar")


def test_parse_eduperson_entitlement_loose():
    e = parse_eduperson_entitlement("foobar", strict=False)
    assert e == Entitlement(None, None, None, None, None)


def test_is_early_adopter():
    assert is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu") is True
    assert is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud:role=Early_Adopter#aai.egi.eu") is True
    assert is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud:role=Early-Adopter#aai.egi.eu") is True
    assert is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud:role=EarlyAdopter#aai.egi.eu") is True
    assert is_early_adopter("urn:mace:egi-dev.eu:group:vo.openeo-dev.cloud:role=early-adopter#aai.egi.eu") is True

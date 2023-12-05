import logging
import re
import time
from typing import List, Tuple

import openeo_driver.config.load
import pytest
import requests
from openeo.rest import OpenEoApiError, OpenEoRestError
from openeo.rest.connection import url_join
from openeo.util import ContextTimer, rfc3339
from openeo_driver.backend import ServiceMetadata
from openeo_driver.errors import (
    JobNotFinishedException,
    JobNotFoundException,
    ProcessGraphInvalidException,
    ProcessGraphMissingException,
    ProcessGraphNotFoundException,
)
from openeo_driver.testing import (
    TEST_USER,
    TEST_USER_AUTH_HEADER,
    TEST_USER_BEARER_TOKEN,
    ApiTester,
    DictSubSet,
    RegexMatcher,
)

import openeo_aggregator.about
from openeo_aggregator.config import AggregatorConfig, get_config_dir
from openeo_aggregator.constants import JOB_OPTION_FORCE_BACKEND
from openeo_aggregator.metadata import (
    STAC_PROPERTY_FEDERATION_BACKENDS,
    STAC_PROPERTY_PROVIDER_BACKEND,
)
from openeo_aggregator.testing import clock_mock

from .conftest import assert_dict_subset, get_api100, get_flask_app


class TestGeneral:
    def test_capabilities(self, api100):
        res = api100.get("/").assert_status_code(200)
        capabilities = res.json
        assert capabilities["api_version"] == "1.0.0"
        assert capabilities["backend_version"] == openeo_aggregator.about.__version__
        endpoints = capabilities["endpoints"]
        assert {"methods": ["GET"], "path": "/collections"} in endpoints
        assert {"methods": ["GET"], "path": "/collections/{collection_id}"} in endpoints
        assert {"methods": ["GET"], "path": "/processes"} in endpoints
        assert capabilities["federation"] == {
            "b1": {"url": "https://b1.test/v1"},
            "b2": {"url": "https://b2.test/v1"},
        }

    def test_title_and_description(self, api100):
        res = api100.get("/").assert_status_code(200)
        capabilities = res.json
        assert capabilities["title"] == "openEO Aggregator Test Dummy"
        assert capabilities["description"] == "openEO Aggregator Test Dummy"

    def test_capabilities_validation(self, api100):
        """https://github.com/Open-EO/openeo-aggregator/issues/42"""
        res = api100.get("/").assert_status_code(200)
        capabilities = res.json
        endpoints = capabilities["endpoints"]
        paths = set(e["path"] for e in endpoints)
        assert "/validation" in paths

    def test_billing_plans(self, api100):
        capabilities = api100.get("/").assert_status_code(200).json
        assert capabilities["billing"] == {
            "currency": "credits",
        }

    def test_deploy_metadata(self, api100):
        capabilities = api100.get("/").assert_status_code(200).json
        assert "openeo_aggregator" in capabilities["processing:software"]


    def test_only_oidc_auth(self, api100):
        res = api100.get("/").assert_status_code(200)
        capabilities = res.json
        endpoints = {e["path"] for e in capabilities["endpoints"]}
        assert {e for e in endpoints if e.startswith("/credentials")} == {"/credentials/basic", "/credentials/oidc"}

    def test_info(self, flask_app):
        api100 = ApiTester(api_version="1.0.0", client=flask_app.test_client(), url_root="/")
        res = api100.get("_info").assert_status_code(200)
        assert res.json == {
            "backends": [{"id": "b1", "root_url": "https://b1.test/v1"}, {"id": "b2", "root_url": "https://b2.test/v1"}]
        }

    def test_health_check_basic(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/health", json={"health": "OK"}, headers={"Content-type": "application/json"})
        requests_mock.get(backend2 + "/health", text="OK")
        resp = api100.get("/health").assert_status_code(200)
        assert resp.json == {
            "backend_status": {
                "b1": {"status_code": 200, "json": {"health": "OK"}, "response_time": pytest.approx(0.1, abs=0.1)},
                "b2": {"status_code": 200, "text": "OK", "response_time": pytest.approx(0.1, abs=0.1)},
            },
            "status_code": 200,
        }

    @pytest.mark.parametrize(["status_code"], [(404,), (500,)])
    def test_health_check_failed_backend(self, api100, requests_mock, backend1, backend2, status_code):
        requests_mock.get(backend1 + "/health", json={"health": "OK"}, headers={"Content-type": "application/json"})
        requests_mock.get(backend2 + "/health", status_code=status_code, text="broken")
        resp = api100.get("/health").assert_status_code(status_code)
        assert resp.json == {
            "backend_status": {
                "b1": {"status_code": 200, "json": {"health": "OK"}, "response_time": pytest.approx(0.1, abs=0.1)},
                "b2": {"status_code": status_code, "text": "broken", "response_time": pytest.approx(0.1, abs=0.1)},
            },
            "status_code": status_code,
        }

    def test_health_check_invalid_backend(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/health", json={"health": "OK"}, headers={"Content-type": "application/json"})
        requests_mock.get(backend2 + "/health", text='Inva{id J}0n', headers={"Content-type": "application/json"})
        resp = api100.get("/health").assert_status_code(500)
        assert resp.json == {
            "backend_status": {
                "b1": {"status_code": 200, "json": {"health": "OK"}, "response_time": pytest.approx(0.1, abs=0.1)},
                "b2": {
                    "status_code": 200,
                    "error": RegexMatcher(r"JSONDecodeError\('Expecting value"),
                    "response_time": pytest.approx(0.1, abs=0.1),
                    "error_time": pytest.approx(0.1, abs=0.1),
                },
            },
            "status_code": 500,
        }


class TestGeneralRealConfig:
    """Some temporary test against real config (that is going to be moved out of this repo at some point)"""

    @pytest.fixture(autouse=True)
    def real_config(self, monkeypatch):
        openeo_driver.config.load._backend_config_getter.flush()
        monkeypatch.setenv(
            openeo_driver.config.load.ConfigGetter.OPENEO_BACKEND_CONFIG,
            str(get_config_dir() / "backend_config.py"),
        )
        yield
        openeo_driver.config.load._backend_config_getter.flush()

    def test_title_and_description(self, api100):
        res = api100.get("/").assert_status_code(200)
        capabilities = res.json
        assert capabilities["title"] == "openEO Platform"
        assert capabilities["description"] == "openEO Platform, provided through openEO Aggregator Driver"

    def test_only_oidc_auth(self, api100):
        res = api100.get("/").assert_status_code(200)
        capabilities = res.json
        endpoints = {e["path"] for e in capabilities["endpoints"]}
        assert {e for e in endpoints if e.startswith("/credentials")} == {"/credentials/oidc"}


class TestCatalog:

    def test_collections_basic(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        res = api100.get("/collections").assert_status_code(200).json
        assert set(c["id"] for c in res["collections"]) == {"S1", "S2", "S3"}

    def test_collections_duplicate(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}, {"id": "S3"}]})
        res = api100.get("/collections").assert_status_code(200).json
        assert set(c["id"] for c in res["collections"]) == {"S1", "S2", "S3"}

    def test_collection_full_metadata(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S1", json={"id": "S1", "title": "b1 S1"})
        requests_mock.get(backend1 + "/collections/S2", json={"id": "S2", "title": "b1 S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        requests_mock.get(backend2 + "/collections/S3", json={"id": "S3", "title": "b2 S3"})

        res = api100.get("/collections/S1").assert_status_code(200).json
        assert res == DictSubSet({"id": "S1", "title": "b1 S1"})

        res = api100.get("/collections/S2").assert_status_code(200).json
        assert res == DictSubSet({"id": "S2", "title": "b1 S2"})

        res = api100.get("/collections/S3").assert_status_code(200).json
        assert res == DictSubSet({"id": "S3", "title": "b2 S3"})

        res = api100.get("/collections/S4")
        res.assert_error(404, "CollectionNotFound")

    def test_collection_items(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})

        def collection_items(request, context):
            assert request.qs == {"bbox": ["5,45,20,50"], "datetime": ["2019-09-20/2019-09-22"], "limit": ["2"]}
            context.headers["Content-Type"] = "application/json"
            return {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": "blabla"}]}

        requests_mock.get(backend1 + "/collections/S1/items", json=collection_items)

        res = api100.get("/collections/S1/items?bbox=5,45,20,50&datetime=2019-09-20/2019-09-22&limit=2")
        res.assert_status_code(200)
        assert res.json == {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": "blabla"}]}

    @pytest.mark.parametrize(["backend1_up", "backend2_up", "expected"], [
        (True, False, {"S1", "S2"}),
        (False, True, {"S3"}),
        (False, False, set()),
    ])
    def test_collections_resilience(
            self, api100, requests_mock, backend1, backend2, backend1_up, backend2_up, expected
    ):
        if backend1_up:
            requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
        else:
            requests_mock.get(backend1 + "/collections", status_code=404, text="down")
        if backend2_up:
            requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        else:
            requests_mock.get(backend2 + "/collections", status_code=404, text="down")

        res = api100.get("/collections").assert_status_code(200).json
        assert set(c["id"] for c in res["collections"]) == expected
        # TODO: test caching of results

    @pytest.mark.parametrize("status_code", [204, 303, 404, 500])
    def test_collection_full_metadata_resilience(self, api100, requests_mock, backend1, backend2, status_code):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        requests_mock.get(backend1 + "/collections/S1", json={"id": "S1", "title": "b1 S1"})
        requests_mock.get(backend1 + "/collections/S2", status_code=status_code, text="down")
        requests_mock.get(backend2 + "/collections/S3", status_code=status_code, text="down")

        res = api100.get("/collections/S1").assert_status_code(200).json
        assert res == DictSubSet({"id": "S1", "title": "b1 S1"})

        api100.get("/collections/S2").assert_error(404, "CollectionNotFound")
        api100.get("/collections/S3").assert_error(404, "CollectionNotFound")
        api100.get("/collections/S4").assert_error(404, "CollectionNotFound")
        # TODO: test caching of results

    def test_collections_links(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(
            backend1 + "/collections",
            json={
                "collections": [
                    {"id": "S1"},
                    {
                        "id": "S2",
                        "links": [
                            {"rel": "license", "href": "foo"},
                            # These parent/root/self links should be overruled in the result
                            {"rel": "parent", "href": "https://s2.test/"},
                            {"rel": "self", "href": "https://s2.test/S2"},
                        ],
                    },
                ]
            },
        )
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        res = api100.get("/collections").assert_status_code(200).json
        assert res == {
            "collections": [
                DictSubSet(
                    {
                        "description": "S1",
                        "links": [
                            {"rel": "root", "href": "http://oeoa.test/openeo/1.0.0/collections"},
                            {"rel": "parent", "href": "http://oeoa.test/openeo/1.0.0/collections"},
                            {"rel": "self", "href": "http://oeoa.test/openeo/1.0.0/collections/S1"},
                        ],
                    }
                ),
                DictSubSet(
                    {
                        "description": "S2",
                        "links": [
                            {"rel": "license", "href": "foo"},
                            {"rel": "root", "href": "http://oeoa.test/openeo/1.0.0/collections"},
                            {"rel": "parent", "href": "http://oeoa.test/openeo/1.0.0/collections"},
                            {"rel": "self", "href": "http://oeoa.test/openeo/1.0.0/collections/S2"},
                        ],
                    }
                ),
                DictSubSet(
                    {
                        "description": "S3",
                        "links": [
                            {"rel": "root", "href": "http://oeoa.test/openeo/1.0.0/collections"},
                            {"rel": "parent", "href": "http://oeoa.test/openeo/1.0.0/collections"},
                            {"rel": "self", "href": "http://oeoa.test/openeo/1.0.0/collections/S3"},
                        ],
                    }
                ),
            ],
            "links": [],
        }

    @pytest.mark.parametrize(
        ["config_override", "expected"],
        [
            ({"collection_whitelist": None}, {"S1", "S2", "S3", "S4"}),
            ({"collection_whitelist": []}, {"S1", "S2", "S3", "S4"}),
            ({"collection_whitelist": ["S2"]}, {"S2"}),
            ({"collection_whitelist": ["S4"]}, {"S4"}),
            ({"collection_whitelist": ["S2", "S3"]}, {"S2", "S3"}),
            ({"collection_whitelist": ["S2", "S999"]}, {"S2"}),
            ({"collection_whitelist": ["S999"]}, set()),
        ],
    )
    def test_collections_whitelist(self, api100, requests_mock, backend1, backend2, expected):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}, {"id": "S3"}]})
        for cid in ["S1", "S2", "S3"]:
            requests_mock.get(backend1 + f"/collections/{cid}", json={"id": cid, "title": f"b1 {cid}"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}, {"id": "S4"}]})
        for cid in ["S3", "S4"]:
            requests_mock.get(backend2 + f"/collections/{cid}", json={"id": cid, "title": f"b2 {cid}"})

        res = api100.get("/collections").assert_status_code(200).json
        assert set(c["id"] for c in res["collections"]) == expected

        res = api100.get("/collections/S2")
        if "S2" in expected:
            assert res.assert_status_code(200).json == DictSubSet({"id": "S2", "title": "b1 S2"})
        else:
            res.assert_error(404, "CollectionNotFound")

        res = api100.get("/collections/S3")
        if "S3" in expected:
            assert res.assert_status_code(200).json == DictSubSet({"id": "S3", "title": "b1 S3"})
        else:
            res.assert_error(404, "CollectionNotFound")

        res = api100.get("/collections/S999")
        res.assert_error(404, "CollectionNotFound")


class TestAuthentication:
    def test_credentials_oidc_default(self, api100, backend1, backend2):
        res = api100.get("/credentials/oidc").assert_status_code(200).json
        assert res == {"providers": [
            {"id": "egi", "issuer": "https://egi.test", "title": "EGI", "scopes": ["openid"]},
            {"id": "x-agg", "issuer": "https://x.test", "title": "X (agg)", "scopes": ["openid"]},
            {"id": "y-agg", "issuer": "https://y.test", "title": "Y (agg)", "scopes": ["openid"]},
            {"id": "z-agg", "issuer": "https://z.test", "title": "Z (agg)", "scopes": ["openid"]},
        ]}

    def test_me_unauthorized(self, api100):
        api100.get("/me").assert_error(401, "AuthenticationRequired")

    def test_me_basic_auth_invalid(self, api100):
        headers = {"Authorization": "Bearer " + "basic//foobar"}
        api100.get("/me", headers=headers).assert_error(403, "TokenInvalid")

    def test_me_basic_auth(self, api100):
        headers = TEST_USER_AUTH_HEADER
        res = api100.get("/me", headers=headers).assert_status_code(200)
        assert res.json["user_id"] == TEST_USER


class TestAuthEntitlementCheck:
    def test_basic_auth(self, api100_with_entitlement_check, caplog):
        api100_with_entitlement_check.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100_with_entitlement_check.get("/me")
        res.assert_error(
            403, "PermissionsInsufficient",
            message="An EGI account is required for using openEO Platform."
        )
        warnings = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
        assert re.search(r"internal_auth_data.*authentication_method.*basic", warnings)

    def test_oidc_no_entitlement_data(self, api100_with_entitlement_check, requests_mock, caplog):
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer funiculifunicula"
            return {"sub": "john"}

        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get("https://egi.test/userinfo", json=get_userinfo)
        api100_with_entitlement_check.set_auth_bearer_token(token="oidc/egi/funiculifunicula")

        res = api100_with_entitlement_check.get("/me")
        res.assert_error(
            403, "PermissionsInsufficient",
            message="Proper enrollment in openEO Platform virtual organization is required."
        )
        warnings = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
        assert re.search(r"KeyError.*eduperson_entitlement", warnings)

    def _get_userifo_handler(self, eduperson_entitlement: List[str], bearer_token: str = "funiculifunicula"):
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == f"Bearer {bearer_token}"
            return {
                "sub": "john",
                "eduperson_entitlement": eduperson_entitlement
            }

        return get_userinfo

    @pytest.mark.parametrize(["eduperson_entitlement", "warn_regex"], [
        (
                [],
                r"eduperson_entitlements['\": ]*\[\]",
        ),
        (
                ["urn:mace:egi.eu:group:vo.openeo.test:role=foo#test"],
                r"eduperson_entitlements.*vo\.openeo\.test:role=foo",
        ),
        (
                ["urn:mace:egi.eu:group:vo.openeo.cloud:role=foo#aai.egi.eu"],
                r"eduperson_entitlements.*vo\.openeo\.cloud:role=foo",
        ),
        (
                [
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=foo#test",
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=member#test",
                ],
                r"eduperson_entitlements.*vo\.openeo\.cloud:role=member",
        )
    ])
    def test_oidc_not_enrolled(
            self, api100_with_entitlement_check, requests_mock, caplog, eduperson_entitlement, warn_regex
    ):
        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get(
            "https://egi.test/userinfo",
            json=self._get_userifo_handler(eduperson_entitlement=eduperson_entitlement)
        )
        api100_with_entitlement_check.set_auth_bearer_token(token="oidc/egi/funiculifunicula")

        res = api100_with_entitlement_check.get("/me")
        res.assert_error(
            403, "PermissionsInsufficient",
            message="Proper enrollment in openEO Platform virtual organization is required."
        )
        warnings = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)
        assert re.search(r"user_id.*john", warnings)
        assert re.search(warn_regex, warnings)

    @pytest.mark.parametrize(
        ["eduperson_entitlement", "expected_roles"],
        [
            (
                [
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=foo#aai.egi.eu",
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=30day-trial#aai.egi.eu",
                ],
                ["30DayTrial"],
            ),
            (
                [
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=foo#aai.egi.eu",
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu",
                ],
                ["EarlyAdopter"],
            ),
            (
                [
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu",
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=basicuser#aai.egi.eu",
                    "urn:mace:egi.eu:group:vo.openeo.cloud:role=Platform-developer#aai.egi.eu",
                ],
                ["BasicUser", "EarlyAdopter", "PlatformDeveloper"],
            ),
        ],
    )
    def test_oidc_enrolled(self, api100_with_entitlement_check, requests_mock, eduperson_entitlement, expected_roles):
        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get(
            "https://egi.test/userinfo",
            json=self._get_userifo_handler(eduperson_entitlement=eduperson_entitlement)
        )
        api100_with_entitlement_check.set_auth_bearer_token(token="oidc/egi/funiculifunicula")

        res = api100_with_entitlement_check.get("/me").assert_status_code(200)
        data = res.json
        assert data["user_id"] == "john"
        assert data["roles"] == expected_roles
        assert "default_plan" not in data

    @pytest.mark.parametrize(["whitelist", "main_test_oidc_issuer", "success"], [
        (["https://egi.test"], "https://egi.test", True),
        (["https://egi.test"], "https://egi.test/", True),
        (["https://egi.test/"], "https://egi.test", True),
        (["https://egi.test/"], "https://egi.test/", True),
        (["https://egi.test/oidc"], "https://egi.test/oidc/", True),
        (["https://egi.test/oidc/"], "https://egi.test/oidc", True),
        (["https://egi.test/foo"], "https://egi.test/bar", False),
    ])
    def test_issuer_url_normalization(
            self, config, requests_mock, backend1, backend2, whitelist,
            main_test_oidc_issuer, success, caplog,
    ):
        config.auth_entitlement_check = {"oidc_issuer_whitelist": whitelist}

        requests_mock.get(backend1 + "/credentials/oidc", json={"providers": [
            {"id": "egi", "issuer": main_test_oidc_issuer, "title": "EGI"}
        ]})
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": "egi", "issuer": main_test_oidc_issuer, "title": "EGI"}
        ]})
        oidc_url_ui = url_join(main_test_oidc_issuer, "/userinfo")
        oidc_url_conf = url_join(main_test_oidc_issuer, "/.well-known/openid-configuration")
        requests_mock.get(oidc_url_conf, json={"userinfo_endpoint": oidc_url_ui})
        requests_mock.get(
            oidc_url_ui,
            json=self._get_userifo_handler(eduperson_entitlement=[
                "urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu",
            ])
        )
        api100 = get_api100(get_flask_app(config))
        api100.set_auth_bearer_token(token="oidc/egi/funiculifunicula")

        if success:
            res = api100.get("/me").assert_status_code(200)
            data = res.json
            assert data["user_id"] == "john"
            assert data["roles"] == ["EarlyAdopter"]
        else:
            res = api100.get("/me")
            res.assert_error(403, "PermissionsInsufficient")
            assert re.search(
                "user_access_validation failure.*oidc_issuer.*https://egi.test/bar.*issuer_whitelist.*https://egi.test/foo",
                caplog.text
            )


class TestProcessing:
    def test_processes_basic(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(
            backend1 + "/processes",
            json={
                "processes": [
                    {
                        "id": "add",
                        "parameters": [
                            {"name": "x", "schema": {"type": "number"}},
                            {"name": "y", "schema": {"type": "number"}},
                        ],
                    },
                    {
                        "id": "mean",
                        "parameters": [{"name": "data", "schema": {"type": "array"}}],
                        "returns": {"schema": {"type": "number"}},
                    },
                ]
            },
        )
        requests_mock.get(
            backend2 + "/processes",
            json={
                "processes": [
                    {
                        "id": "multiply",
                        "parameters": [
                            {"name": "x", "schema": {"type": "number"}},
                            {"name": "y", "schema": {"type": "number"}},
                        ],
                    },
                    {
                        "id": "mean",
                        "parameters": [{"name": "data", "schema": {"type": "array"}}],
                    },
                ]
            },
        )
        res = api100.get("/processes").assert_status_code(200).json
        assert res == {
            "processes": [
                {
                    "id": "add",
                    "description": "add",
                    "parameters": [
                        {"name": "x", "schema": {"type": "number"}, "description": "x"},
                        {"name": "y", "schema": {"type": "number"}, "description": "y"},
                    ],
                    "returns": {"schema": {}},
                    "federation:backends": ["b1"],
                    'deprecated': False,
                    'experimental': False,
                    'examples': [],
                    'links': []
                },
                {
                    "id": "mean",
                    "description": "mean",
                    "parameters": [
                        {
                            "name": "data",
                            "schema": {"type": "array"},
                            "description": "data",
                        }
                    ],
                    "returns": {"schema": {"type": "number"}},
                    "federation:backends": ["b1", "b2"],
                    'deprecated': False,
                    'experimental': False,
                    'examples': [],
                    'links': []
                },
                {
                    "id": "multiply",
                    "description": "multiply",
                    "parameters": [
                        {"name": "x", "schema": {"type": "number"}, "description": "x"},
                        {"name": "y", "schema": {"type": "number"}, "description": "y"},
                    ],
                    "returns": {"schema": {}},
                    "federation:backends": ["b2"],
                    'deprecated': False,
                    'experimental': False,
                    'examples': [],
                    'links': []
                },
            ],
            "links": [],
        }

    @pytest.mark.parametrize(
        ["backend1_up", "backend2_up", "expected"],
        [
            (
                True,
                False,
                [
                    {
                        "id": "add",
                        "description": "add",
                        "parameters": [
                            {"name": "x", "schema": {}, "description": "x"},
                            {"name": "y", "schema": {}, "description": "y"},
                        ],
                        "returns": {"schema": {}},
                        "federation:backends": ["b1"],
                        'deprecated': False,
                        'experimental': False,
                        'examples': [],
                        'links': []
                    },
                    {
                        "id": "mean",
                        "description": "mean",
                        "parameters": [
                            {"name": "data", "schema": {}, "description": "data"}
                        ],
                        "returns": {"schema": {}},
                        "federation:backends": ["b1"],
                        'deprecated': False,
                        'experimental': False,
                        'examples': [],
                        'links': []
                    },
                ],
            ),
            (
                False,
                True,
                [
                    {
                        "id": "mean",
                        "description": "mean",
                        "parameters": [{"name": "data", "schema": {}, "description": "data"}],
                        "returns": {"schema": {}},
                        "federation:backends": ["b2"],
                        'deprecated': False,
                        'experimental': False,
                        'examples': [],
                        'links': []
                    },
                    {
                        "id": "multiply",
                        "description": "multiply",
                        "parameters": [
                            {"name": "x", "schema": {}, "description": "x"},
                            {"name": "y", "schema": {}, "description": "y"},
                        ],
                        "returns": {"schema": {}},
                        "federation:backends": ["b2"],
                        'deprecated': False,
                        'experimental': False,
                        'examples': [],
                        'links': []
                    },
                ],
            ),
            (False, False, []),
        ],
    )
    def test_processes_resilience(
        self,
        api100,
        requests_mock,
        backend1,
        backend2,
        backend1_up,
        backend2_up,
        expected,
    ):
        if backend1_up:
            requests_mock.get(
                backend1 + "/processes",
                json={
                    "processes": [
                        {
                            "id": "add",
                            "parameters": [
                                {"name": "x", "schema": {}},
                                {"name": "y", "schema": {}},
                            ],
                        },
                        {"id": "mean", "parameters": [{"name": "data", "schema": {}}]},
                    ]
                },
            )
        else:
            requests_mock.get(backend1 + "/processes", status_code=404, text="nope")
        if backend2_up:
            requests_mock.get(
                backend2 + "/processes",
                json={
                    "processes": [
                        {
                            "id": "multiply",
                            "parameters": [
                                {"name": "x", "schema": {}},
                                {"name": "y", "schema": {}},
                            ],
                        },
                        {"id": "mean", "parameters": [{"name": "data", "schema": {}}]},
                    ]
                },
            )
        else:
            requests_mock.get(backend2 + "/processes", status_code=404, text="nope")
        res = api100.get("/processes").assert_status_code(200).json
        assert res == {"processes": expected, "links": []}

    def test_result_basic_math_basic_auth(self, api100, requests_mock, backend1, backend2):
        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            pg = request.json()["process"]["process_graph"]
            (_, node), = pg.items()
            assert node["process_id"] == "add"
            assert node["result"] is True
            context.headers["Content-Type"] = "application/json"
            return node["arguments"]["x"] + node["arguments"]["y"]

        requests_mock.post(backend1 + "/result", json=post_result)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 8

    def test_result_basic_math_oidc_auth(self, api100, requests_mock, backend1, backend2):
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer funiculifunicula"
            return {"sub": "john"}

        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer oidc/egi/funiculifunicula"
            pg = request.json()["process"]["process_graph"]
            (_, node), = pg.items()
            assert node["process_id"] == "add"
            assert node["result"] is True
            context.headers["Content-Type"] = "application/json"
            return node["arguments"]["x"] + node["arguments"]["y"]

        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get("https://egi.test/userinfo", json=get_userinfo)

        requests_mock.post(backend1 + "/result", json=post_result)
        api100.set_auth_bearer_token(token="oidc/egi/funiculifunicula")
        pg = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 8

    @pytest.mark.parametrize("status_code", [201, 302, 404, 500])
    def test_result_basic_math_error(self, api100, requests_mock, backend1, backend2, status_code):
        requests_mock.post(backend1 + "/result", status_code=status_code, text="nope")
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request)
        res.assert_error(500, "Internal", message="Failed to process synchronously on backend b1")

    @pytest.mark.parametrize(["chunk_size"], [(16,), (128,)])
    def test_result_large_response_streaming(self, config, chunk_size, requests_mock, backend1, backend2):
        config.streaming_chunk_size = chunk_size
        api100 = get_api100(get_flask_app(config))

        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            assert request.json()["process"]["process_graph"] == pg
            context.headers["Content-Type"] = "application/octet-stream"
            return bytes(b % 256 for b in range(1000))

        requests_mock.post(backend1 + "/result", content=post_result)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"large": {"process_id": "large", "arguments": {}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)

        assert res.response.is_streamed
        chunks = res.response.iter_encoded()
        first_chunk = next(chunks)
        assert len(first_chunk) == chunk_size
        assert first_chunk.startswith(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09")
        assert len(next(chunks)) == chunk_size
        assert len(res.data) == 1000 - 2 * chunk_size

    @pytest.mark.parametrize(["cid", "call_counts"], [
        ("S1", (1, 0)),
        ("S10", (1, 0)),
        ("S2", (0, 1)),
        ("S20", (0, 1)),
    ])
    def test_result_backend_by_collection(self, api100, requests_mock, backend1, backend2, cid, call_counts):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S10"}, ]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}, {"id": "S20"}, ]})

        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            assert request.json()["process"]["process_graph"] == pg
            context.headers["Content-Type"] = "application/json"
            return 123

        b1_mock = requests_mock.post(backend1 + "/result", json=post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=post_result)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": cid}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 123
        assert (b1_mock.call_count, b2_mock.call_count) == call_counts

    def test_processes_different_versions(
        self, api100, requests_mock, backend1, backend2, mbldr
    ):
        """
        This used to fail with
            OpenEOApiException: Only single version is supported, but found: {'1.2.3', '1.3.5'}
        """
        requests_mock.get(backend1 + "/", json=mbldr.capabilities(api_version="1.2.3"))
        requests_mock.get(backend2 + "/", json=mbldr.capabilities(api_version="1.3.5"))
        requests_mock.get(backend1 + "/processes", json=mbldr.processes("add", "mean"))
        requests_mock.get(backend2 + "/processes", json=mbldr.processes("prod", "mean"))

        res = api100.get("/processes").assert_status_code(200).json
        assert res == {
            "processes": [
                DictSubSet({"id": "add", "federation:backends": ["b1"]}),
                DictSubSet({"id": "mean", "federation:backends": ["b1", "b2"]}),
                DictSubSet({"id": "prod", "federation:backends": ["b2"]}),
            ],
            "links": [],
        }

    def test_result_backend_by_collection_multiple_hits(self, api100, requests_mock, backend1, backend2, caplog):
        caplog.set_level(logging.WARNING)
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}, {"id": "S2"}, ]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}, {"id": "S3"}, ]})

        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            assert request.json()["process"]["process_graph"] == pg
            context.headers["Content-Type"] = "application/json"
            return 123

        b1_mock = requests_mock.post(backend1 + "/result", json=post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=post_result)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 123
        assert (b1_mock.call_count, b2_mock.call_count) == (1, 0)

        assert "Multiple back-end candidates ['b1', 'b2'] for collections {'S2'}." in caplog.text
        assert "Naively picking first one" in caplog.text

    def test_result_backend_by_collection_collection_not_found(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": "S3"}, "result": True}}
        res = api100.post("/result", json={"process": {"process_graph": pg}})
        res.assert_error(404, "CollectionNotFound", "Collection 'S3' does not exist")

    @pytest.mark.parametrize("pg", [
        {"lc": {}},
        {"lc": {"foo": "bar"}},
        {"lc": {"process_id": "load_collection"}},
        {"lc": {"process_id": "load_collection", "arguments": {}}},
    ])
    def test_result_backend_by_collection_invalid_pg(self, api100, requests_mock, backend1, backend2, pg):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/result", json={"process": {"process_graph": pg}})
        res.assert_error(400, "ProcessGraphInvalid")

    @pytest.mark.parametrize(
        ["user_selected_backend", "expected_response", "expected_call_counts"],
        [
            ("b1", (200, None), (1, 0)),
            ("b2", (200, None), (0, 1)),
            ("b3", (400, "BackendLookupFailure"), (0, 0)),
        ],
    )
    @pytest.mark.parametrize(
        "stac_property",
        [
            STAC_PROPERTY_PROVIDER_BACKEND,
            STAC_PROPERTY_FEDERATION_BACKENDS,
        ],
    )
    def test_load_collection_from_user_selected_backend(
        self,
        api100,
        backend1,
        backend2,
        requests_mock,
        user_selected_backend,
        expected_response,
        expected_call_counts,
        stac_property,
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={"id": "S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={"id": "S2"})

        def post_result(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            assert request.json()["process"]["process_graph"] == pg
            context.headers["Content-Type"] = "application/json"
            return 123

        b1_mock = requests_mock.post(backend1 + "/result", json=post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=post_result)

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {
            "lc": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S2",
                    "properties": {
                        stac_property: {
                            "process_graph": {
                                "eq": {
                                    "process_id": "eq",
                                    "arguments": {
                                        "x": {"from_parameter": "value"},
                                        "y": user_selected_backend,
                                    },
                                    "result": True,
                                }
                            }
                        }
                    },
                },
                "result": True,
            }
        }
        request = {"process": {"process_graph": pg}}
        response = api100.post("/result", json=request)

        expected_status, expected_error_code = expected_response
        if expected_status < 400:
            response.assert_status_code(expected_status)
        else:
            response.assert_error(status_code=expected_status, error_code=expected_error_code)

        assert (b1_mock.call_count, b2_mock.call_count) == expected_call_counts

    def test_load_result_job_id_parsing_basic(self, api100, requests_mock, backend1, backend2):
        """Issue #19: strip backend prefix from job_id in load_result"""

        def b1_post_result(request: requests.Request, context):
            pg = request.json()["process"]["process_graph"]
            assert pg == {"load": {"process_id": "load_result", "arguments": {"id": "b6tch-j08"}, "result": True}}
            context.headers["Content-Type"] = "application/json"
            return 111

        def b2_post_result(request: requests.Request, context):
            pg = request.json()["process"]["process_graph"]
            assert pg == {"load": {"process_id": "load_result", "arguments": {"id": "897c5-108"}, "result": True}}
            context.headers["Content-Type"] = "application/json"
            return 222

        b1_mock = requests_mock.post(backend1 + "/result", json=b1_post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=b2_post_result)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        pg = {"load": {"process_id": "load_result", "arguments": {"id": "b1-b6tch-j08"}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 111
        assert (b1_mock.call_count, b2_mock.call_count) == (1, 0)

        pg = {"load": {"process_id": "load_result", "arguments": {"id": "b2-897c5-108"}, "result": True}}
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 222
        assert (b1_mock.call_count, b2_mock.call_count) == (1, 1)

    @pytest.mark.parametrize(["job_id", "s2_backend", "expected_success"], [
        ("b1-b6tch-j08", 1, True),
        ("b2-b6tch-j08", 1, False),
        ("b1-b6tch-j08", 2, False),
        ("b2-b6tch-j08", 2, True),
    ])
    def test_load_result_job_id_parsing_with_load_collection(
            self, api100, requests_mock, backend1, backend2, job_id, s2_backend, expected_success
    ):
        """Issue #19: strip backend prefix from job_id in load_result"""

        backend_root = {1: backend1, 2: backend2}[s2_backend]
        requests_mock.get(backend_root + "/collections", json={"collections": [{"id": "S2"}]})

        def post_result(request: requests.Request, context):
            pg = request.json()["process"]["process_graph"]
            assert pg["lr"]["arguments"]["id"] == "b6tch-j08"
            context.headers["Content-Type"] = "application/json"

        b1_mock = requests_mock.post(backend1 + "/result", json=post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=post_result)

        pg = {
            "lr": {"process_id": "load_result", "arguments": {"id": job_id}},
            "lc": {"process_id": "load_collection", "arguments": {"id": "S2"}},
        }
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        request = {"process": {"process_graph": pg}}
        if expected_success:
            api100.post("/result", json=request).assert_status_code(200)
            assert (b1_mock.call_count, b2_mock.call_count) == {1: (1, 0), 2: (0, 1)}[s2_backend]
        else:
            api100.post("/result", json=request).assert_error(400, "BackendLookupFailure")
            assert (b1_mock.call_count, b2_mock.call_count) == (0, 0)

    @pytest.mark.parametrize(["job_id", "s2_backend", "expected_success"], [
        ("b1-b6tch-j08", 1, True),
        ("b2-b6tch-j08", 1, False),
        ("b1-b6tch-j08", 2, False),
        ("b2-b6tch-j08", 2, True),
        ("https://example.com/ml_model_metadata.json", 1, True),  # In this case it picks the first backend.
        ("https://example.com/ml_model_metadata.json", 2, True),
    ])
    def test_load_result_job_id_parsing_with_load_ml_model(
            self, api100, requests_mock, backend1, backend2, job_id, s2_backend, expected_success
    ):
        """Issue #70: random forest: providing training job with aggregator job id fails"""

        backend_root = {1: backend1, 2: backend2}[s2_backend]
        requests_mock.get(backend_root + "/collections", json={"collections": [{"id": "S2"}]})

        def post_result(request: requests.Request, context):
            pg = request.json()["process"]["process_graph"]
            assert pg["lmm"]["arguments"]["id"] in ["b6tch-j08", "https://example.com/ml_model_metadata.json"]
            context.headers["Content-Type"] = "application/json"

        b1_mock = requests_mock.post(backend1 + "/result", json=post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=post_result)

        pg = {
            "lmm": {"process_id": "load_ml_model", "arguments": {"id": job_id}},
            "lc": {"process_id": "load_collection", "arguments": {"id": "S2"}},
        }
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        request = {"process": {"process_graph": pg}}
        if expected_success:
            api100.post("/result", json=request).assert_status_code(200)
            assert (b1_mock.call_count, b2_mock.call_count) == {1: (1, 0), 2: (0, 1)}[s2_backend]
        else:
            api100.post("/result", json=request).assert_error(400, "BackendLookupFailure")
            assert (b1_mock.call_count, b2_mock.call_count) == (0, 0)

    @pytest.mark.parametrize(
        "result_id",
        [
            "https://oeoa.test/jobs/b1-b6tch-j08/results",
            "http://external.test/bla/bla",
            "https://external.test/bla/bla",
        ],
    )
    def test_load_result_http_reference(
        self, api100, requests_mock, backend1, backend2, result_id
    ):
        """Support load_result with HTTP references (instead of job id)"""

        def b1_post_result(request: requests.Request, context):
            pg = request.json()["process"]["process_graph"]
            assert pg == {
                "load": {
                    "process_id": "load_result",
                    "arguments": {"id": result_id},
                    "result": True,
                }
            }
            context.headers["Content-Type"] = "application/json"
            return 111

        b1_mock = requests_mock.post(backend1 + "/result", json=b1_post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json={"dummy": "dummy"})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        pg = {
            "load": {
                "process_id": "load_result",
                "arguments": {"id": result_id},
                "result": True,
            }
        }
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 111
        assert (b1_mock.call_count, b2_mock.call_count) == (1, 0)

    @pytest.mark.parametrize(
        ["process_id", "call_counts", "expected_warnings"],
        [
            (
                "blargh",
                (1, 0),
                [
                    RegexMatcher(
                        "Multiple back-end candidates.*Naively picking first one"
                    )
                ],
            ),
            ("wibble", (1, 0), []),
            ("snorfle", (0, 1), []),
            (
                "frobnicate",
                (1, 0),
                [
                    RegexMatcher("Skipping unknown process 'frobnicate'"),
                    RegexMatcher(
                        "Multiple back-end candidates.*Naively picking first one"
                    ),
                ],
            ),
        ],
    )
    def test_result_backend_by_process(
        self,
        api100,
        requests_mock,
        backend1,
        backend2,
        process_id,
        call_counts,
        caplog,
        mbldr,
        expected_warnings,
    ):
        requests_mock.get(backend1 + "/collections", json=mbldr.collections("S2"))
        common_processes = [{"id": "load_collection"}, {"id": "blargh"}]
        requests_mock.get(
            backend1 + "/processes",
            json={"processes": common_processes + [{"id": "wibble"}]},
        )
        requests_mock.get(backend2 + "/collections", json=mbldr.collections("S2"))
        requests_mock.get(
            backend2 + "/processes",
            json={"processes": common_processes + [{"id": "snorfle"}]},
        )

        def post_result(request: requests.Request, context):
            assert (
                request.headers["Authorization"]
                == TEST_USER_AUTH_HEADER["Authorization"]
            )
            assert request.json()["process"]["process_graph"] == pg
            context.headers["Content-Type"] = "application/json"
            return 123

        b1_mock = requests_mock.post(backend1 + "/result", json=post_result)
        b2_mock = requests_mock.post(backend2 + "/result", json=post_result)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        pg = {
            "lc": {
                "process_id": "load_collection",
                "arguments": {"id": "S2"},
            },
            "process": {
                "process_id": process_id,
                "arguments": {"data": {"from_node": "lc"}, "factor": 5},
                "result": True,
            },
        }
        request = {"process": {"process_graph": pg}}
        res = api100.post("/result", json=request).assert_status_code(200)
        assert res.json == 123
        assert (b1_mock.call_count, b2_mock.call_count) == call_counts
        assert caplog.messages == expected_warnings

    def test_validation_basic(self, api100, requests_mock, backend1):
        def post_validation(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            assert request.json() == {
                "process_graph": {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
            }
            context.headers["Content-Type"] = "application/json"
            return {"errors": [{"code": "NoMath", "message": "No math support"}]}

        validation_mock = requests_mock.post(backend1 + "/validation", json=post_validation)

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        post_data = {"process_graph": {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}}
        res = api100.post("/validation", json=post_data).assert_status_code(200)
        assert res.json == {"errors": [{"code": "NoMath", "message": "No math support"}]}
        assert validation_mock.call_count == 1

    @pytest.mark.parametrize(
        ["collection_id", "expected_error", "expected_call_counts"],
        [
            ("S1", {"code": "NoData", "message": "No data for S1"}, (1, 0)),
            ("S2", {"code": "NoData", "message": "No data for S2"}, (0, 1)),
            (
                "MEH",
                {
                    "code": "InternalValidationFailure",
                    "message": RegexMatcher(
                        r"^Validation failed: CollectionNotFoundException\(status_code=404, code='CollectionNotFound', message=\"Collection 'MEH' does not exist."
                    ),
                },
                (0, 0),
            ),
        ],
    )
    def test_validation_collection_support(
        self, api100, requests_mock, backend1, backend2, collection_id, expected_error, expected_call_counts
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S1"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})

        def post_validation(request: requests.Request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            collection_id = request.json()["process_graph"]["lc"]["arguments"]["id"]
            context.headers["Content-Type"] = "application/json"
            return {"errors": [{"code": "NoData", "message": f"No data for {collection_id}"}]}

        b1_validation_mock = requests_mock.post(backend1 + "/validation", json=post_validation)
        b2_validation_mock = requests_mock.post(backend2 + "/validation", json=post_validation)

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        post_data = {
            "process_graph": {
                "lc": {"process_id": "load_collection", "arguments": {"id": collection_id}, "result": True}
            }
        }
        res = api100.post("/validation", json=post_data).assert_status_code(200)
        assert res.json == {"errors": [expected_error]}
        assert (b1_validation_mock.call_count, b2_validation_mock.call_count) == expected_call_counts

    def test_validation_upstream_failure(self, api100, requests_mock, backend1, backend2):
        validation_mock = requests_mock.post(backend1 + "/validation", content=b"this is not JSON")

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        post_data = {"process_graph": {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}}
        res = api100.post("/validation", json=post_data).assert_status_code(200)
        assert res.json == {
            "errors": [
                {
                    "code": "UpstreamValidationFailure",
                    "message": "Validation failed on backend b1: JSONDecodeError('Expecting value: line 1 column 1 (char 0)')",
                }
            ]
        }
        assert validation_mock.call_count == 1


class TestBatchJobs:

    def test_list_jobs_no_auth(self, api100):
        api100.get("/jobs").assert_error(401, "AuthenticationRequired")

    def test_list_jobs_basic(self, api100, requests_mock, backend1, backend2):
        requests_mock.get(backend1 + "/jobs", json={"jobs": [
            {"id": "job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
            {"id": "job08", "status": "running", "created": "2021-06-08T12:34:56Z", "title": "Job number 8."},
        ]})
        requests_mock.get(backend2 + "/jobs", json={"jobs": [
            {"id": "job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
        ]})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs").assert_status_code(200).json
        assert res == {
            "jobs": [
                {"id": "b1-job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
                {"id": "b1-job08", "status": "running", "created": "2021-06-08T12:34:56Z", "title": "Job number 8."},
                {"id": "b2-job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
            ],
            "links": [],
        }

    def test_list_jobs_auth(self, api100, requests_mock, backend1, backend2):
        def b1_get_jobs(request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            return {
                "jobs": [
                    {"id": "job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
                    {"id": "job08", "status": "running", "created": "2021-06-08T12:34:56Z", "title": "Job number 8."},
                ]
            }

        def b2_get_jobs(request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            return {
                "jobs": [
                    {"id": "job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
                ]
            }

        requests_mock.get(backend1 + "/jobs", json=b1_get_jobs)
        requests_mock.get(backend2 + "/jobs", json=b2_get_jobs)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs").assert_status_code(200).json
        assert res == {
            "jobs": [
                {"id": "b1-job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
                {"id": "b1-job08", "status": "running", "created": "2021-06-08T12:34:56Z", "title": "Job number 8."},
                {"id": "b2-job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
            ],
            "links": [],
        }

    @pytest.mark.parametrize("b2_oidc_pid", ["egi", "aho"])
    def test_list_jobs_oidc_pid_mapping(self, config, requests_mock, backend1, backend2, b2_oidc_pid):
        # Override /credentials/oidc of backend2 before building flask app and ApiTester
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": b2_oidc_pid, "issuer": "https://egi.test", "title": "EGI"}
        ]})
        api100 = get_api100(get_flask_app(config))

        # OIDC setup
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer t0k3n"
            return {"sub": "john"}

        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get("https://egi.test/userinfo", json=get_userinfo)

        def b1_get_jobs(request, context):
            assert request.headers["Authorization"] == "Bearer oidc/egi/t0k3n"
            return {"jobs": [
                {"id": "job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
                {"id": "job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
            ]}

        def b2_get_jobs(request, context):
            assert request.headers["Authorization"] == f"Bearer oidc/{b2_oidc_pid}/t0k3n"
            return {"jobs": [
                {"id": "job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
            ]}

        requests_mock.get(backend1 + "/jobs", json=b1_get_jobs)
        requests_mock.get(backend2 + "/jobs", json=b2_get_jobs)

        api100.set_auth_bearer_token(token="oidc/egi/t0k3n")
        res = api100.get("/jobs").assert_status_code(200).json
        assert res["jobs"] == [
            {"id": "b1-job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
            {"id": "b1-job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
            {"id": "b2-job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
        ]

    @pytest.mark.parametrize("status_code", [204, 303, 404, 500])
    def test_list_jobs_failing_backend(self, api100, requests_mock, backend1, backend2, caplog, status_code):
        requests_mock.get(backend1 + "/jobs", json={"jobs": [
            {"id": "job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
            {"id": "job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
        ]})
        requests_mock.get(backend2 + "/jobs", status_code=status_code, json={"code": "nope", "message": "and nope"})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs").assert_status_code(200).json
        assert res == {
            "jobs": [
                {"id": "b1-job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
                {"id": "b1-job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
            ],
            "links": [],
            "federation:missing": ["b2"],
        }

        warnings = "\n".join(r.msg for r in caplog.records if r.levelno == logging.WARNING)
        assert "Failed to get job listing from backend 'b2'" in warnings

    def test_list_jobs_offline_backend(self, api100, requests_mock, backend1, backend2, caplog):
        requests_mock.get(backend1 + "/jobs", json={"jobs": [
            {"id": "job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
            {"id": "job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
        ]})
        requests_mock.get(backend2 + "/", status_code=500, json={"code": "nope", "message": "completely down!"})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # Wait for connections cache to expire
        with clock_mock(offset=1000):
            res = api100.get("/jobs").assert_status_code(200).json
            assert res == {
                "jobs": [
                    {"id": "b1-job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
                    {"id": "b1-job08", "status": "running", "created": "2021-06-08T12:34:56Z"},
                ],
                "links": [],
                "federation:missing": ["b2"],
            }

            warnings = "\n".join(r.msg for r in caplog.records if r.levelno == logging.WARNING)
            assert "Failed to create backend 'b2' connection" in warnings

    def test_list_jobs_invalid_metadata(self, api100, requests_mock, backend1, backend2, caplog):
        """https://github.com/Open-EO/openeo-aggregator/issues/109"""
        requests_mock.get(
            backend1 + "/jobs",
            json={"jobs": [{"id": "job03", "status": "running", "created": "2021-06-03T12:34:56Z"}]},
        )
        requests_mock.get(
            backend2 + "/jobs",
            json={"jobs": [{"id": "job05", "status": "running", "created": "not a date obviously"}]},
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs").assert_status_code(200).json
        assert res == {
            "jobs": [{"id": "b1-job03", "status": "running", "created": "2021-06-03T12:34:56Z"}],
            "links": [],
        }

        errors = "\n".join(r.msg for r in caplog.records if r.levelno == logging.ERROR)
        assert "get_user_jobs: skipping job with parse issue" in errors

    def test_list_jobs_fractional_second_parsing(self, api100, requests_mock, backend1, backend2):
        """https://github.com/Open-EO/openeo-aggregator/issues/109"""
        requests_mock.get(
            backend1 + "/jobs",
            json={"jobs": [{"id": "job03", "status": "running", "created": "2021-06-03T12:34:56Z"}]},
        )
        requests_mock.get(
            backend2 + "/jobs",
            json={"jobs": [{"id": "job05", "status": "running", "created": "2021-06-05T12:34:56.789Z"}]},
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs").assert_status_code(200).json
        assert res == {
            "jobs": [
                {"id": "b1-job03", "status": "running", "created": "2021-06-03T12:34:56Z"},
                {"id": "b2-job05", "status": "running", "created": "2021-06-05T12:34:56Z"},
            ],
            "links": [],
        }

    def test_create_job_basic(self, api100, requests_mock, backend1):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})

        jobs = []

        def post_jobs(request: requests.Request, context):
            nonlocal jobs
            jobs.append(request.json())
            context.headers["Location"] = backend1 + "/jobs/th3j0b"
            context.headers["OpenEO-Identifier"] = "th3j0b"
            context.status_code = 201

        requests_mock.post(backend1 + "/jobs", text=post_jobs)

        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}}
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json={"process": {"process_graph": pg}}).assert_status_code(201)
        assert res.headers["Location"] == "http://oeoa.test/openeo/1.0.0/jobs/b1-th3j0b"
        assert res.headers["OpenEO-Identifier"] == "b1-th3j0b"
        assert jobs == [
            {"process": {"process_graph": pg}}
        ]

    def test_create_job_options(self, api100, requests_mock, backend1):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})

        def post_jobs(request: requests.Request, context):
            assert request.json() == {
                "process": {"process_graph": pg},
                "title": "da job",
                "plan": "free-tier",
                "job_options": {"side": "salad"},
            }
            context.headers["Location"] = backend1 + "/jobs/th3j0b"
            context.headers["OpenEO-Identifier"] = "th3j0b"
            context.status_code = 201

        requests_mock.post(backend1 + "/jobs", text=post_jobs)

        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}}
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json={
            "process": {"process_graph": pg},
            "title": "da job",
            "plan": "free-tier",
            "job_options": {"side": "salad"},
            "something else": "whatever",
        }).assert_status_code(201)
        assert res.headers["Location"] == "http://oeoa.test/openeo/1.0.0/jobs/b1-th3j0b"
        assert res.headers["OpenEO-Identifier"] == "b1-th3j0b"

    @pytest.mark.parametrize("body", [
        {"foo": "meh"},
        {"process": "meh"},
    ])
    def test_create_job_pg_missing(self, api100, requests_mock, backend1, body):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json=body)
        res.assert_error(400, "ProcessGraphMissing")

    @pytest.mark.parametrize("body", [
        {"process": {"process_graph": "meh"}},
        {"process": {"process_graph": {}}},
        {"process": {"process_graph": {"foo": "meh"}}},
        {"process": {"process_graph": {"foo": {"bar": "meh"}}}},
        {"process": {"process_graph": {"foo": {"process_id": "meh"}}}},
    ])
    def test_create_job_pg_invalid(self, api100, requests_mock, backend1, body):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.post(
            backend1 + "/jobs",
            status_code=ProcessGraphInvalidException.status_code,
            json=ProcessGraphInvalidException().to_dict(),
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json=body)
        res.assert_error(400, "ProcessGraphInvalid")

    @pytest.mark.parametrize("status_code", [200, 201, 500])
    def test_create_job_backend_failure(self, api100, requests_mock, backend1, status_code):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})

        def post_jobs(request: requests.Request, context):
            # Go wrong here: missing headers or unexpected status code
            context.status_code = status_code

        requests_mock.post(backend1 + "/jobs", text=post_jobs)

        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}}
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json={"process": {"process_graph": pg}})
        res.assert_error(500, "Internal", message="Failed to create job on backend 'b1'")

    @pytest.mark.parametrize(
        ["force_backend", "expected"],
        [("b1", "b1"), ("b2", "b2"), (None, "b1")],
    )
    def test_create_job_force_backend(
        self, api100, requests_mock, backend1, backend2, force_backend, expected
    ):
        requests_mock.get(
            backend1 + "/collections", json={"collections": [{"id": "S2"}]}
        )
        requests_mock.get(
            backend2 + "/collections", json={"collections": [{"id": "S2"}]}
        )

        jobs = []

        def post_jobs(request: requests.Request, context):
            nonlocal jobs
            jobs.append(request.json())
            context.headers["Location"] = backend1 + "/jobs/th3j0b"
            context.headers["OpenEO-Identifier"] = "th3j0b"
            context.status_code = 201

        backend1_post_jobs = requests_mock.post(backend1 + "/jobs", text=post_jobs)
        backend2_post_jobs = requests_mock.post(backend2 + "/jobs", text=post_jobs)

        pg = {
            "lc": {
                "process_id": "load_collection",
                "arguments": {"id": "S2"},
                "result": True,
            }
        }
        job_options = {}

        if force_backend:
            job_options[JOB_OPTION_FORCE_BACKEND] = force_backend
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post(
            "/jobs",
            json={
                "process": {"process_graph": pg},
                "job_options": job_options,
            },
        ).assert_status_code(201)
        assert (
            res.headers["Location"]
            == f"http://oeoa.test/openeo/1.0.0/jobs/{expected}-th3j0b"
        )
        assert res.headers["OpenEO-Identifier"] == f"{expected}-th3j0b"
        assert jobs == [{"process": {"process_graph": pg}}]

        assert (backend1_post_jobs.call_count, backend2_post_jobs.call_count) == {
            "b1": (1, 0),
            "b2": (0, 1),
        }[expected]

    def test_create_job_no_auto_validation(self, api100, requests_mock, backend1, caplog):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})

        jobs = []

        def post_jobs(request: requests.Request, context):
            nonlocal jobs
            jobs.append(request.json())
            context.headers["Location"] = backend1 + "/jobs/th3j0b"
            context.headers["OpenEO-Identifier"] = "th3j0b"
            context.status_code = 201

        requests_mock.post(backend1 + "/jobs", text=post_jobs)
        validation_mock = requests_mock.post(
            backend1 + "/validation", status_code=500, json={"code": "Internal", "message": "Validation says no"}
        )

        pg = {"lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}}
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json={"process": {"process_graph": pg}}).assert_status_code(201)
        assert res.headers["Location"] == "http://oeoa.test/openeo/1.0.0/jobs/b1-th3j0b"
        assert res.headers["OpenEO-Identifier"] == "b1-th3j0b"
        assert jobs == [{"process": {"process_graph": pg}}]
        assert "Validation says no" not in caplog.text
        assert validation_mock.call_count == 0

    def test_get_job_metadata(self, api100, requests_mock, backend1):
        requests_mock.get(backend1 + "/jobs/th3j0b", json={
            "id": "th3j0b",
            "title": "The job", "description": "Just doing my job.",
            "process": {"process_graph": {
                "lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}
            }},
            "status": "running", "progress": 42, "created": "2017-01-01T09:32:12Z",
            "usage": {
                "cpu": {"value": 1000, "unit": "cpu-seconds"},
                "memory": {"value": 2000, "unit": "mb-seconds"},
                "duration": {"value": 3000, "unit": "seconds"},
            },
        })
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b").assert_status_code(200)
        assert res.json == {
            "id": "b1-th3j0b",
            "title": "The job", "description": "Just doing my job.",
            "process": {"process_graph": {
                "lc": {"process_id": "load_collection", "arguments": {"id": "S2"}, "result": True}
            }},
            "status": "running", "progress": 42, "created": "2017-01-01T09:32:12Z",
            "usage": {
                "cpu": {"value": 1000, "unit": "cpu-seconds"},
                "memory": {"value": 2000, "unit": "mb-seconds"},
                "duration": {"value": 3000, "unit": "seconds"},
            },
        }

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_get_job_metadata_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        requests_mock.get(
            backend1 + f"/jobs/{job_id}",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get(f"/jobs/b1-{job_id}")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")

    def test_get_job_metadata_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/nope-and-nope")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_start_job(self, api100, requests_mock, backend1):
        m = requests_mock.post(backend1 + "/jobs/th3j0b/results", status_code=202)
        requests_mock.get(backend1 + "/jobs/th3j0b", json={
            "id": "th3j0b", "status": "created", "created": "2017-01-01T09:32:12Z",
        })
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        api100.post("/jobs/b1-th3j0b/results").assert_status_code(202)
        assert m.call_count == 1

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_start_job_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        m = requests_mock.get(
            backend1 + f"/jobs/{job_id}",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post(f"/jobs/b1-{job_id}/results")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")
        assert m.call_count == 1

    def test_start_job_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs/nope-and-nope/results")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_cancel_job(self, api100, requests_mock, backend1):
        m = requests_mock.delete(backend1 + "/jobs/th3j0b/results", status_code=204)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        api100.delete("/jobs/b1-th3j0b/results").assert_status_code(204)
        assert m.call_count == 1

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_cancel_job_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        m = requests_mock.delete(
            backend1 + f"/jobs/{job_id}/results",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.delete(f"/jobs/b1-{job_id}/results")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")
        assert m.call_count == 1

    def test_cancel_job_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.delete("/jobs/nope-and-nope/results")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_delete_job(self, api100, requests_mock, backend1):
        m = requests_mock.delete(backend1 + "/jobs/th3j0b", status_code=204)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        api100.delete("/jobs/b1-th3j0b").assert_status_code(204)
        assert m.call_count == 1

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_delete_job_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        m = requests_mock.delete(
            backend1 + f"/jobs/{job_id}",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.delete(f"/jobs/b1-{job_id}")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")
        assert m.call_count == 1

    def test_delete_job_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.delete("/jobs/nope-and-nope")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_get_results(self, api100, requests_mock, backend1):
        m1 = requests_mock.get(backend1 + "/jobs/th3j0b", json={
            "id": "th3j0b",
            "title": "The job", "description": "Just doing my job.",
            "status": "finished", "progress": 100, "created": "2017-01-01T09:32:12Z",
        })
        m2 = requests_mock.get(backend1 + "/jobs/th3j0b/results", status_code=200, json={
            "assets": {
                "r1.tiff": {
                    "href": "https//res.b1.test/123/r1.tiff",
                    "title": "Result 1",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data", "testing"],
                }
            }
        })
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b/results").assert_status_code(200).json
        assert m1.call_count == 1
        assert m2.call_count == 1
        assert res["assets"] == {
            "r1.tiff": {
                "href": "https//res.b1.test/123/r1.tiff",
                "title": "Result 1",
                "roles": ["data", "testing"],
                "file:nodata": [None],
                "type": "image/tiff; application=geotiff",
            }
        }
        assert res["id"] == "b1-th3j0b"
        assert res["type"] == "Feature"
        assert_dict_subset(
            {"title": "The job", "created": "2017-01-01T09:32:12Z", "description": "Just doing my job."},
            res["properties"]
        )

    @pytest.mark.parametrize("job_status", ["created", "running", "canceled", "error"])
    def test_get_results_not_finished(self, api100, requests_mock, backend1, job_status):
        requests_mock.get(backend1 + "/jobs/th3j0b", json={
            "id": "th3j0b", "status": job_status, "created": "2017-01-01T09:32:12Z",
        })
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b/results")
        res.assert_error(JobNotFinishedException.status_code, "JobNotFinished")

    def test_get_results_finished_unreliable(self, api100, requests_mock, backend1):
        """Edge case: job status is 'finished', but results still return with 'JobNotFinished'."""
        m1 = requests_mock.get(backend1 + "/jobs/th3j0b", json={
            "id": "th3j0b", "status": "finished", "created": "2017-01-01T09:32:12Z",
        })
        m2 = requests_mock.get(
            backend1 + "/jobs/th3j0b/results",
            status_code=JobNotFinishedException.status_code, json=JobNotFinishedException().to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b/results")
        res.assert_error(JobNotFinishedException.status_code, "JobNotFinished")
        assert m1.call_count == 1
        assert m2.call_count == 1

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_get_results_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        requests_mock.get(
            backend1 + f"/jobs/{job_id}",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get(f"/jobs/b1-{job_id}/results")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")

    def test_get_results_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/nope-and-nope/results")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_get_results_canonical_link(self, api100, requests_mock, backend1):
        """https://github.com/Open-EO/openeo-aggregator/issues/98"""
        m1 = requests_mock.get(
            backend1 + "/jobs/th3j0b",
            json={
                "id": "th3j0b",
                "status": "finished",
                "created": "2017-01-01T09:32:12Z",
            },
        )
        m2 = requests_mock.get(
            backend1 + "/jobs/th3j0b/results",
            status_code=200,
            json={
                "assets": {},
                "links": [
                    {
                        "rel": "canonical",
                        "href": "https://res.b1.test/123/456789/abc",
                        "type": "application/json",
                    }
                ],
            },
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b/results").assert_status_code(200).json
        assert res["id"] == "b1-th3j0b"
        assert [l for l in res["links"] if l["rel"] == "canonical"] == [
            {
                "rel": "canonical",
                "href": "https://res.b1.test/123/456789/abc",
                "type": "application/json",
            },
        ]
        assert m1.call_count == 1
        assert m2.call_count == 1

    def test_get_results_links(self, api100, requests_mock, backend1):
        """https://github.com/Open-EO/openeo-aggregator/issues/98"""
        m1 = requests_mock.get(
            backend1 + "/jobs/th3j0b",
            json={
                "id": "th3j0b",
                "status": "finished",
                "created": "2017-01-01T09:32:12Z",
            },
        )
        m2 = requests_mock.get(
            backend1 + "/jobs/th3j0b/results",
            status_code=200,
            json={
                "assets": {},
                "links": [
                    {"rel": "canonical", "href": "https://c.b1.test/123"},
                    {"rel": "self", "href": "https://b1.test/123"},
                ],
            },
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b/results").assert_status_code(200).json
        assert res["id"] == "b1-th3j0b"
        # Preserve original "canonical" link
        assert [l for l in res["links"] if l["rel"] == "canonical"] == [
            {
                "rel": "canonical",
                "href": "https://c.b1.test/123",
            },
        ]
        # Aggregator's "self" link
        assert [l for l in res["links"] if l["rel"] == "self"] == [
            {
                "rel": "self",
                "href": "http://oeoa.test/openeo/1.0.0/jobs/b1-th3j0b/results",
                "type": "application/json",
            },
        ]
        assert m1.call_count == 1
        assert m2.call_count == 1

    def test_get_logs(self, api100, requests_mock, backend1):
        def get_logs(request, context):
            offset = request.qs.get("offset", ["_"])[0]
            return {"logs": [
                {"id": offset + "1", "level": "info", "message": "hello"},
                {"id": offset + "11", "level": "info", "message": "hello"},
            ]}

        requests_mock.get(backend1 + "/jobs/th3j0b/logs", status_code=200, json=get_logs)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/b1-th3j0b/logs").assert_status_code(200).json
        assert res == {
            "logs": [
                {"id": "_1", "level": "info", "message": "hello"},
                {"id": "_11", "level": "info", "message": "hello"},
            ],
            "links": []
        }

        res = api100.get("/jobs/b1-th3j0b/logs?offset=3").assert_status_code(200).json
        assert res == {
            "logs": [
                {"id": "31", "level": "info", "message": "hello"},
                {"id": "311", "level": "info", "message": "hello"},
            ],
            "links": []
        }

    @pytest.mark.parametrize("job_id", ["th3j0b", "th-3j-0b", "th.3j.0b", "th~3j~0b"])
    def test_get_logs_not_found_on_backend(self, api100, requests_mock, backend1, job_id):
        requests_mock.get(
            backend1 + f"/jobs/{job_id}/logs",
            status_code=JobNotFoundException.status_code, json=JobNotFoundException(job_id=job_id).to_dict()
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get(f"/jobs/b1-{job_id}/logs")
        res.assert_error(404, "JobNotFound", message=f"The batch job 'b1-{job_id}' does not exist.")

    def test_get_logs_not_found_on_aggregator(self, api100):
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/jobs/nope-and-nope/logs")
        res.assert_error(404, "JobNotFound", message="The batch job 'nope-and-nope' does not exist.")

    def test_create_job_preprocessing(self, api100, requests_mock, backend1):
        """Issue #19: strip backend prefix from job_id in load_result"""
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})

        jobs = []

        def post_jobs(request: requests.Request, context):
            nonlocal jobs
            jobs.append(request.json())
            context.headers["Location"] = backend1 + "/jobs/th3j0b"
            context.headers["OpenEO-Identifier"] = "th3j0b"
            context.status_code = 201

        requests_mock.post(backend1 + "/jobs", text=post_jobs)

        pg = {"load": {"process_id": "load_result", "arguments": {"id": "b1-b6tch-j08"}, "result": True}}
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.post("/jobs", json={"process": {"process_graph": pg}}).assert_status_code(201)
        assert res.headers["Location"] == "http://oeoa.test/openeo/1.0.0/jobs/b1-th3j0b"
        assert res.headers["OpenEO-Identifier"] == "b1-th3j0b"

        assert jobs == [
            {"process": {"process_graph": {
                "load": {"process_id": "load_result", "arguments": {"id": "b6tch-j08"}, "result": True}
            }}}
        ]


class TestSecondaryServices:

    @pytest.fixture
    def service_metadata_wmts_foo(self):
        return ServiceMetadata(
            id="wmts-foo",
            process={"process_graph": {"foo": {"process_id": "foo", "arguments": {}}}},
            url='https://oeo.net/wmts/foo',
            type="WMTS",
            enabled=True,
            configuration={"version": "0.5.8"},
            attributes={},
            title="Test WMTS service"
            # not setting "created": This is used to test creating a service.
        )

    SERVICE_TYPES_ONLT_WMTS = {
        "WMTS": {
            "configuration": {
                "colormap": {
                    "default": "YlGn",
                    "description":
                    "The colormap to apply to single band layers",
                    "type": "string"
                },
                "version": {
                    "default": "1.0.0",
                    "description": "The WMTS version to use.",
                    "enum": ["1.0.0"],
                    "type": "string"
                }
            },
            "links": [],
            "process_parameters": [],
            "title": "Web Map Tile Service"
        }
    }

    def test_service_types_simple(self, api100, backend1, requests_mock, mbldr):
        """Given 2 backends but only 1 backend has a single service, then the aggregator
            returns that 1 service's metadata.
        """

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(
            backend1 + "/", json=mbldr.capabilities(secondary_services=True)
        )
        # Only need a single service type.
        single_service_type = self.SERVICE_TYPES_ONLT_WMTS
        requests_mock.get(backend1 + "/service_types", json=single_service_type)

        resp = api100.get('/service_types').assert_status_code(200)
        assert resp.json == single_service_type

    def test_service_types_multiple_backends(
        self, api100, backend1, backend2, requests_mock, mbldr
    ):
        """Given 2 backends with each 1 service, then the aggregator lists both services."""

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(
            backend1 + "/", json=mbldr.capabilities(secondary_services=True)
        )
        requests_mock.get(
            backend2 + "/", json=mbldr.capabilities(secondary_services=True)
        )
        service_type_1 = {
            "WMTS": {
                "configuration": {
                    "colormap": {
                        "default": "YlGn",
                        "description":
                        "The colormap to apply to single band layers",
                        "type": "string"
                    },
                    "version": {
                        "default": "1.0.0",
                        "description": "The WMTS version to use.",
                        "enum": ["1.0.0"],
                        "type": "string"
                    }
                },
                "links": [],
                "process_parameters": [],
                "title": "Web Map Tile Service"
            }
        }
        service_type_2 = {
            "WMS": {
                "title": "OGC Web Map Service",
                "configuration": {},
                "process_parameters": [],
                "links": []
            }
        }
        requests_mock.get(backend1 + "/service_types", json=service_type_1)
        requests_mock.get(backend2 + "/service_types", json=service_type_2)

        resp = api100.get("/service_types").assert_status_code(200)
        actual_service_types = resp.json

        expected_service_types = dict(service_type_1)
        expected_service_types.update(service_type_2)
        assert actual_service_types == expected_service_types

    def test_service_info(self, api100, backend1, requests_mock):
        """When it gets a correct service ID, it returns the expected service's metadata as JSON."""

        json_wmts_foo = {
            "id": "wmts-foo",
            "process": {"process_graph": {"foo": {"process_id": "foo", "arguments": {}}}},
            "url": "https://oeo.net/wmts/foo",
            "type": "WMTS",
            "enabled": "True",
            "configuration": {"version": "0.5.8"},
            "attributes": {},
            "title": "Test WMTS service"
        }
        requests_mock.get(backend1 + "/services/wmts-foo", json=json_wmts_foo)
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        resp = api100.get("/services/b1-wmts-foo").assert_status_code(200)

        expected_json_wmts_foo = dict(json_wmts_foo)
        expected_json_wmts_foo["id"] = "b1-" + json_wmts_foo["id"]
        assert resp.json == expected_json_wmts_foo

    def test_service_info_wrong_id(self, api100):
        """When it gets a non-existent service ID, the aggregator responds with HTTP 404, not found."""
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)

        # The backend ID is wrong.
        api100.get("/services/doesnotexist-someservice").assert_status_code(404)

        # The backend ID exists but the service ID is wrong.
        api100.get("/services/b1-doesnotexist").assert_status_code(404)

    def test_list_services_only_1_backend(self, api100, requests_mock, backend1, mbldr):
        """
        Given 2 backends but only 1 backend has a single service, then the aggregator
        returns that 1 service's metadata.
        """

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(
            backend1 + "/", json=mbldr.capabilities(secondary_services=True)
        )
        requests_mock.get(
            backend1 + "/services",
            json={
                "services": [
                    {
                        "id": "service-123",
                        "url": "https://maps.test/123",
                        "type": "XYZ",
                    }
                ],
                "links": [],
            },
        )

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        response = api100.get("/services").assert_status_code(200).json
        assert response == {
            "services": [
                {
                    "id": "b1-service-123",
                    "type": "XYZ",
                    "url": "https://maps.test/123",
                    "enabled": True,
                }
            ],
            "links": [],
        }

    def test_list_services_no_supporting_backends(
        self, api100, requests_mock, backend1, caplog
    ):
        """None of the upstream backends supports secondary services"""
        caplog.set_level(logging.ERROR)
        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)

        # No backends supported ==> Rely on default mock in backend1 for capabilities at "GET /"
        # But the backend's /services endpoint should not be called in this scenario,
        # so make it a fail in a loud way if the aggregator *does* call it.
        mock_backend_services = requests_mock.get(
            backend1 + "/services",
            exc=Exception("Backend 1's /services should not be reached in this test."),
        )

        response = api100.get("/services").assert_status_code(200).json

        assert response == {
            "services": [],
            "links": [],
        }

        # Should not reach the backend in this case
        assert not mock_backend_services.called

        # And it should not log any errors either.
        # We could make the assert more specific and only fail if there are error messages.
        # But we can be reasonably sure that in this test any log message would be an error message.
        # You could check for error messages via caplog.record_tuples which is a
        # list of (logger_name, level, message) tuples.
        assert not caplog.messages

    def test_list_services_basic(
        self, api100, requests_mock, backend1, backend2, mbldr
    ):
        """
        Given 2 backends with each 1 service, then the aggregator lists both services.
        """

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(
            backend1 + "/", json=mbldr.capabilities(secondary_services=True)
        )
        requests_mock.get(
            backend2 + "/", json=mbldr.capabilities(secondary_services=True)
        )
        requests_mock.get(
            backend1 + "/services",
            json={
                "services": [
                    {
                        "id": "service-123",
                        "url": "https://maps.test/123",
                        "type": "XYZ",
                        "enabled": True,
                        "attributes": {"layers": ["ndvi", "evi"]},
                        "configuration": {"version": "2.3.5"},
                        "title": "Service 123",
                        "created": rfc3339.datetime(2022, 12, 1),
                    }
                ],
                "links": [],
            },
        )
        requests_mock.get(
            backend2 + "/services",
            json={
                "services": [
                    {
                        "id": "my-wms",
                        "url": "https://wms.test/yo",
                        "type": "WMS",
                        "title": "My WMS",
                    }
                ],
                "links": [],
            },
        )

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        response = api100.get("/services").assert_status_code(200).json
        assert response == {
            "services": [
                {
                    "id": "b1-service-123",
                    "type": "XYZ",
                    "url": "https://maps.test/123",
                    "created": "2022-12-01T00:00:00Z",
                    "enabled": True,
                    "title": "Service 123",
                    "configuration": {"version": "2.3.5"},
                },
                {
                    "id": "b2-my-wms",
                    "type": "WMS",
                    "url": "https://wms.test/yo",
                    "enabled": True,
                    "title": "My WMS",
                },
            ],
            "links": [],
        }

    def test_list_services_extended(
        self, api100, requests_mock, backend1, backend2, mbldr
    ):
        """
        Given multiple services across 2 backends, the aggregator lists all service types from all backends.
        """

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(
            backend1 + "/", json=mbldr.capabilities(secondary_services=True)
        )
        requests_mock.get(
            backend2 + "/", json=mbldr.capabilities(secondary_services=True)
        )
        requests_mock.get(
            backend1 + "/services",
            json={
                "services": [
                    {
                        "id": "wms-nvdi",
                        "title": "NDVI based on Sentinel 2",
                        "description": "Deriving minimum NDVI measurements over pixel time series of Sentinel 2",
                        "url": "https://example.openeo.org/wms/wms-nvdi",
                        "type": "wms",
                        "enabled": True,
                        "process": {
                            "id": "ndvi",
                            "summary": "string",
                            "description": "string",
                            "links": [
                                {
                                    "rel": "related",
                                    "href": "https://example.openeo.org",
                                    "type": "text/html",
                                    "title": "openEO",
                                }
                            ],
                            "process_graph": {
                                "foo": {"process_id": "foo", "arguments": {}}
                            },
                        },
                        "configuration": {"version": "1.3.0"},
                        "attributes": {"layers": ["ndvi", "evi"]},
                        "created": "2017-01-01T09:32:12Z",
                    }
                ],
                "links": [
                    {
                        "rel": "related",
                        "href": "https://example.openeo.org",
                        "type": "text/html",
                        "title": "openEO",
                    }
                ],
            },
        )
        requests_mock.get(
            backend2 + "/services",
            json={
                "services": [
                    {
                        "id": "my-wms",
                        "url": "https://wms.test/yo",
                        "type": "WMS",
                    },
                    {
                        "id": "another-wms",
                        "url": "https://wms.test/wasup",
                        "type": "WMS",
                    },
                ],
                "links": [],
            },
        )

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        response = api100.get("/services").assert_status_code(200).json
        assert response == {
            "services": [
                {
                    "id": "b1-wms-nvdi",
                    "type": "wms",
                    "url": "https://example.openeo.org/wms/wms-nvdi",
                    "configuration": {"version": "1.3.0"},
                    "created": "2017-01-01T09:32:12Z",
                    "description": "Deriving minimum NDVI measurements over pixel "
                    "time series of Sentinel 2",
                    "enabled": True,
                    "title": "NDVI based on Sentinel 2",
                },
                {
                    "id": "b2-my-wms",
                    "type": "WMS",
                    "url": "https://wms.test/yo",
                    "enabled": True,
                },
                {
                    "id": "b2-another-wms",
                    "type": "WMS",
                    "url": "https://wms.test/wasup",
                    "enabled": True,
                },
            ],
            # TODO: merging of `links` field
            "links": [],
        }

    def test_create_wmts(self, api100, requests_mock, backend1, mbldr):
        """When the payload is correct the service should be successfully created,
        the service ID should be prepended with the backend ID,
        and location should point to the aggregator, not to the backend directly.
        """
        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(
            backend1 + "/", json=mbldr.capabilities(secondary_services=True)
        )

        backend_service_id = 'c63d6c27-c4c2-4160-b7bd-9e32f582daec'
        expected_agg_id = f"b1-{backend_service_id}"

        # The aggregator MUST NOT point to the backend instance but to its own endpoint.
        # This is handled by the openeo python driver in openeo_driver.views.services_post.
        expected_location = f"/openeo/1.0.0/services/{expected_agg_id}"
        # However, backend1 must report its OWN location.
        location_backend_1 = f"{backend1}/services/{backend_service_id}"

        process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
        post_data = {
            "type": 'WMTS',
            "process": {
                "process_graph": process_graph,
                "id": "filter_temporal_wmts"
            },
            "title": "My Service",
            "description": "Service description"
        }
        requests_mock.post(
            backend1 + "/services",
            headers={
                "OpenEO-Identifier": backend_service_id,
                "Location": location_backend_1
            },
            status_code=201
        )
        requests_mock.get(backend1 + "/service_types", json=self.SERVICE_TYPES_ONLT_WMTS)

        resp = api100.post('/services', json=post_data).assert_status_code(201)

        assert resp.headers["OpenEO-Identifier"] == expected_agg_id
        assert resp.headers["Location"] == expected_location

    # ProcessGraphMissingException and ProcessGraphInvalidException are well known reasons for a bad client request.
    @pytest.mark.parametrize("exception_class", [ProcessGraphMissingException, ProcessGraphInvalidException])
    def test_create_wmts_reports_400_client_error(self, api100, requests_mock, backend1, exception_class):
        """When the backend raises exceptions that are typically a bad request / HTTP 400, then
        we expect the aggregator to return a HTTP 400 status code."""

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
        post_data = {
            "type": 'WMTS',
            "process": {
                "process_graph": process_graph,
                "id": "filter_temporal_wmts"
            },
            "title": "My Service",
            "description": "Service description"
        }
        # TODO: In theory we should make the backend report a HTTP 400 status and then the aggregator
        # should also report HTTP 400. But in fact that comes back as HTTP 500.
        requests_mock.post(
            backend1 + "/services",
            exc=exception_class("Testing exception handling")
        )

        resp = api100.post('/services', json=post_data)
        assert resp.status_code == 400

    # OpenEoApiError, OpenEoRestError: more general errors we can expect to lead to a HTTP 500 server error.
    @pytest.mark.parametrize("exception_class", [OpenEoApiError, OpenEoRestError])
    def test_create_wmts_reports_500_server_error(
        self, api100, requests_mock, backend1, exception_class, mbldr
    ):
        """When the backend raises exceptions that are typically a server error / HTTP 500, then
        we expect the aggregator to return a HTTP 500 status code."""

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)

        # Aggregator checks if the backend supports GET /service_types, so we have to mock that up too.
        requests_mock.get(
            backend1 + "/", json=mbldr.capabilities(secondary_services=True)
        )
        process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
        post_data = {
            "type": 'WMTS',
            "process": {
                "process_graph": process_graph,
                "id": "filter_temporal_wmts"
            },
            "title": "My Service",
            "description": "Service description"
        }
        requests_mock.post(
            backend1 + "/services",
            exc=exception_class("Testing exception handling")
        )
        requests_mock.get(backend1 + "/service_types", json=self.SERVICE_TYPES_ONLT_WMTS)

        resp = api100.post('/services', json=post_data)
        assert resp.status_code == 500

    def test_remove_service_succeeds(self, api100, requests_mock, backend1):
        """When remove_service is called with an existing service ID, it removes service and returns HTTP 204."""

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        mock_delete = requests_mock.delete(backend1 + "/services/wmts-foo", status_code=204)

        resp = api100.delete("/services/b1-wmts-foo")

        assert resp.status_code == 204
        # Make sure the aggregator asked the backend to remove the service.
        assert mock_delete.called

    def test_remove_service_but_backend_id_not_found(self, api100):
        """When the service ID does not exist then the aggregator responds with HTTP 404, not found."""

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)

        resp = api100.delete("/services/wmts-foo")

        assert resp.status_code == 404

    def test_remove_service_but_service_id_not_found(self, api100, backend1, requests_mock):
        """When the service ID does not exist then the aggregator responds with HTTP 404, not found."""

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        mock_delete = requests_mock.delete(
            backend1 + "/services/wmts-foo",
            status_code=404,
        )

        resp = api100.delete("/services/b1-wmts-foo")

        assert resp.status_code == 404
        assert mock_delete.called

    def test_remove_service_backend_response_is_an_error_status(
        self, api100, requests_mock, backend1, service_metadata_wmts_foo
    ):
        """When the backend response is an error, HTTP 500, then the aggregator also responds with HTTP 500 status."""

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        requests_mock.get(
            backend1 + "/services/wmts-foo",
            json=service_metadata_wmts_foo.prepare_for_json(),
            status_code=200
        )
        mock_delete = requests_mock.delete(
            backend1 + "/services/wmts-foo",
            status_code=500,
            json={
                "id": "936DA01F-9ABD-4D9D-80C7-02AF85C822A8",
                "code": "ErrorRemovingService",
                "message": "Service 'wmts-foo' could not be removed.",
                "url": "https://example.openeo.org/docs/errors/SampleError"
            }
        )

        resp = api100.delete("/services/b1-wmts-foo")

        assert resp.status_code == 500
        # Verify the aggregator effectively asked the backend to remove the service,
        # so we can reasonably assume that is where the error came from.
        assert mock_delete.called

    def test_update_service_service_succeeds(
        self, api100, backend1, requests_mock, service_metadata_wmts_foo
    ):
        """When it receives an existing service ID and a correct payload, it updates the expected service."""

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)

        mock_patch = requests_mock.patch(backend1 + "/services/wmts-foo", status_code=204)
        process_graph = {"bar": {"process_id": "bar", "arguments": {"new_arg": "somevalue"}}}
        json_payload = {"process": {"process_graph": process_graph}}

        resp = api100.patch("/services/b1-wmts-foo", json=json_payload)

        assert resp.status_code == 204
        # Make sure the aggregator asked the backend to update the service.
        assert mock_patch.called
        assert mock_patch.last_request.json() == json_payload

    def test_update_service_but_backend_id_not_found(self, api100):
        """When the service ID does not exist because the backend prefix is wrong, then the aggregator responds with HTTP 404, not found."""

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        process_graph = {"bar": {"process_id": "bar", "arguments": {"new_arg": "somevalue"}}}
        json_payload = {"process": {"process_graph": process_graph}}

        resp = api100.patch("/services/backenddoesnotexist-someservice", json=json_payload)

        assert resp.status_code == 404

    def test_update_service_service_id_not_found(
        self, api100, backend1, requests_mock, service_metadata_wmts_foo
    ):
        """When the service ID does not exist for the specified backend, then the aggregator responds with HTTP 404, not found."""

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        mock_patch = requests_mock.patch(
            backend1 + "/services/servicedoesnotexist",
            json=service_metadata_wmts_foo.prepare_for_json(),
            status_code=404
        )
        process_graph = {"bar": {"process_id": "bar", "arguments": {"new_arg": "somevalue"}}}
        json_payload = {"process": {"process_graph": process_graph}}

        resp = api100.patch("/services/b1-servicedoesnotexist", json=json_payload)

        assert resp.status_code == 404
        assert mock_patch.called

    # TODO: for now, not bothering with HTTP 400 in the backend. To be decided if this is necessary.
    @pytest.mark.parametrize("backend_http_status", [500])
    def test_update_service_backend_response_is_an_error_status(
        self, api100, backend1, requests_mock, backend_http_status
    ):
        """When the backend response is an error HTTP 400/500 then the aggregator raises an OpenEoApiError."""

        api100.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        mock_patch = requests_mock.patch(
            backend1 + "/services/wmts-foo",
            status_code=backend_http_status,
            json={
                "id": "936DA01F-9ABD-4D9D-80C7-02AF85C822A8",
                "code": "ErrorUpdatingService",
                "message": "Service 'wmts-foo' could not be updated.",
                "url": "https://example.openeo.org/docs/errors/SampleError"
            }
        )
        process_graph = {"bar": {"process_id": "bar", "arguments": {"new_arg": "somevalue"}}}
        json_payload = {"process": {"process_graph": process_graph}}

        resp = api100.patch("/services/b1-wmts-foo", json=json_payload)

        assert resp.status_code == backend_http_status
        assert mock_patch.called
        assert mock_patch.last_request.json() == json_payload


class TestUserDefinedProcesses:
    _UDP_EVI = {
        "id": "evi",
        "summary": "Enhanced Vegetation Index",
        "description": "Computes the Enhanced Vegetation Index (EVI).",
        "parameters": [
            {"name": "red", "description": "Value from the red band.", "schema": {"type": "number"}},
            {"name": "blue", "description": "Value from the blue band.", "schema": {"type": "number"}},
            {"name": "nir", "description": "Value from the near infrared band.", "schema": {"type": "number"}},
        ],
        "returns": {"description": "Computed EVI.", "schema": {"type": "number"}},
    }

    def _with_expected_auth_headers(self, data: dict):
        """Helper to build a dynamic requests_mock response handler that also checks for auth headers"""

        def handle(request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            return data

        return handle

    def test_list_udps_no_auth(self, api100):
        api100.get("/process_graphs").assert_error(401, "AuthenticationRequired")

    def test_list_udps_empty(self, api100, requests_mock, backend1):
        upstream = requests_mock.get(
            backend1 + "/process_graphs", status_code=200, json=self._with_expected_auth_headers({"processes": []})
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/process_graphs").assert_status_code(200).json
        assert res == {"processes": [], "links": []}
        assert upstream.call_count == 1

    def test_list_udps_existing(self, api100, requests_mock, backend1):
        upstream = requests_mock.get(
            backend1 + "/process_graphs",
            json=self._with_expected_auth_headers(
                {
                    "processes": [
                        # A full UDP metadata entry
                        self._UDP_EVI,
                        # A Minimal UDP metadata entry
                        {"id": "somethingelse"},
                    ],
                    "links": [],
                }
            ),
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/process_graphs").assert_status_code(200).json
        assert res == {
            "processes": [
                self._UDP_EVI,
                {"id": "somethingelse"},
            ],
            "links": [],
        }
        assert upstream.call_count == 1

    def test_get_existing(self, api100, requests_mock, backend1):
        upstream = requests_mock.get(
            backend1 + "/process_graphs/evi", json=self._with_expected_auth_headers(self._UDP_EVI)
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        res = api100.get("/process_graphs/evi").assert_status_code(200).json
        expected = self._UDP_EVI.copy()
        assert res == expected
        assert upstream.call_count == 1

    def test_get_non_existing(self, api100, requests_mock, backend1):
        upstream = requests_mock.get(
            backend1 + "/process_graphs/evi",
            status_code=ProcessGraphNotFoundException.status_code,
            json=self._with_expected_auth_headers(ProcessGraphNotFoundException(process_graph_id="dummy").to_dict()),
        )
        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        api100.get("/process_graphs/evi").assert_error(
            status_code=404, error_code="ProcessGraphNotFound", message="'evi' does not exist"
        )
        assert upstream.call_count == 1

    def test_store(self, api100, requests_mock, backend1):
        udp_id = "add35"
        data = {
            "id": udp_id,
            "process_graph": {
                "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True},
            },
        }

        def handle_put(request, context):
            assert request.headers["Authorization"] == TEST_USER_AUTH_HEADER["Authorization"]
            assert request.json() == data
            context.status_code = 200
            return {}

        upstream = requests_mock.put(backend1 + f"/process_graphs/{udp_id}", json=handle_put)

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        api100.put(f"/process_graphs/{udp_id}", json=data).assert_status_code(200)
        assert upstream.call_count == 1

    def test_delete_existing(self, api100, requests_mock, backend1):
        upstream = requests_mock.delete(
            backend1 + f"/process_graphs/evi", status_code=204, json=self._with_expected_auth_headers({})
        )

        api100.set_auth_bearer_token(token=TEST_USER_BEARER_TOKEN)
        api100.delete(f"/process_graphs/evi").assert_status_code(204)
        assert upstream.call_count == 1


class TestResilience:

    @pytest.fixture
    def broken_backend2(
            self, backend1, requests_mock, base_config
    ) -> Tuple[str, AggregatorConfig, 'requests_mock.adapter._Matcher']:
        """Fixture to quickly set up a config with broken backend2"""
        backend2 = "https://b2.test/v1"
        # TODO: return 500 on all requests?
        root_mock = requests_mock.get(backend2 + "/", status_code=500)

        config = base_config.copy()
        config.aggregator_backends = {"b1": backend1, "b2": backend2}
        return backend2, config, root_mock

    def test_startup_during_backend_downtime(self, backend1, broken_backend2, requests_mock, caplog):
        caplog.set_level(logging.WARNING)

        # Initial backend setup with broken backend2
        requests_mock.get(backend1 + "/health", text="OK")
        backend2, config, b2_root = broken_backend2
        api100 = get_api100(get_flask_app(config))

        api100.get("/").assert_status_code(200)

        resp = api100.get("/health").assert_status_code(200)
        assert resp.json == {
            "backend_status": {
                "b1": {"status_code": 200, "text": "OK", "response_time": pytest.approx(0.1, abs=0.1)},
            },
            "status_code": 200,
        }

    def test_startup_during_backend_downtime_and_recover(self, backend1, broken_backend2, requests_mock):
        # Initial backend setup with broken backend2
        requests_mock.get(backend1 + "/health", text="OK")
        backend2, config, b2_root = broken_backend2
        api100 = get_api100(get_flask_app(config))

        assert api100.get("/health").assert_status_code(200).json["backend_status"] == {
            "b1": {"status_code": 200, "text": "OK", "response_time": pytest.approx(0.1, abs=0.1)},
        }

        # Backend 2 is up again, but cached is still active
        requests_mock.get(backend2 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": "egi", "issuer": "https://egi.test", "title": "EGI"}
        ]})
        requests_mock.get(backend2 + "/health", text="ok again")
        assert api100.get("/health").assert_status_code(200).json["backend_status"] == {
            "b1": {"status_code": 200, "text": "OK", "response_time": pytest.approx(0.1, abs=0.1)},
        }

        # Wait a bit so that cache is flushed
        with clock_mock(offset=1000):
            assert api100.get("/health").assert_status_code(200).json["backend_status"] == {
                "b1": {"status_code": 200, "text": "OK", "response_time": pytest.approx(0.1, abs=0.1)},
                "b2": {"status_code": 200, "text": "ok again", "response_time": pytest.approx(0.1, abs=0.1)},
            }

    @pytest.mark.parametrize("b2_oidc_provider_id", ["egi", "aho"])
    def test_oidc_mapping_after_recover(self, backend1, broken_backend2, requests_mock, b2_oidc_provider_id):
        # Initial backend setup with broken backend2
        backend2, config, b2_root = broken_backend2
        api100 = get_api100(get_flask_app(config))

        # OIDC setup
        def get_userinfo(request: requests.Request, context):
            assert request.headers["Authorization"] == "Bearer t0k3n"
            return {"sub": "john"}

        requests_mock.get("https://egi.test/.well-known/openid-configuration", json={
            "userinfo_endpoint": "https://egi.test/userinfo"
        })
        requests_mock.get("https://egi.test/userinfo", json=get_userinfo)

        # Job listings: backend1 works, backend2 is down
        requests_mock.get(backend1 + "/jobs", json={"jobs": [
            {"id": "j0b1", "status": "running", "created": "2021-01-11T11:11:11Z"}
        ]})
        requests_mock.get(backend2 + "/jobs", status_code=500, text="nope")

        api100.set_auth_bearer_token(token="oidc/egi/t0k3n")
        jobs = api100.get("/jobs").assert_status_code(200).json
        assert jobs["jobs"] == [
            {"id": "b1-j0b1", "status": "running", "created": "2021-01-11T11:11:11Z"}
        ]

        # Backend2 is up again (but still cached as down)
        requests_mock.get(backend2 + "/", json={"api_version": "1.0.0"})
        requests_mock.get(backend2 + "/credentials/oidc", json={"providers": [
            {"id": b2_oidc_provider_id, "issuer": "https://egi.test", "title": "EGI"}
        ]})

        def get_jobs(request, context):
            assert request.headers["Authorization"] == f"Bearer oidc/{b2_oidc_provider_id}/t0k3n"
            return {"jobs": [
                {"id": "j0b2", "status": "running", "created": "2021-02-22T22:22:22Z"}
            ]}

        requests_mock.get(backend2 + "/jobs", json=get_jobs)

        jobs = api100.get("/jobs").assert_status_code(200).json
        assert jobs["jobs"] == [
            {"id": "b1-j0b1", "status": "running", "created": "2021-01-11T11:11:11Z"}
        ]

        # Skip time so that connection cache is cleared
        with clock_mock(offset=1000):
            jobs = api100.get("/jobs").assert_status_code(200).json
            assert jobs["jobs"] == [
                {"id": "b1-j0b1", "status": "running", "created": "2021-01-11T11:11:11Z"},
                {"id": "b2-j0b2", "status": "running", "created": "2021-02-22T22:22:22Z"},
            ]

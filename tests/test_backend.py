import pytest

from openeo_aggregator.backend import AggregatorCollectionCatalog, AggregatorProcessing, \
    AggregatorBackendImplementation, _InternalCollectionMetadata, JobIdMapping
from openeo_aggregator.caching import DictMemoizer
from openeo_aggregator.testing import clock_mock
from openeo_driver.errors import OpenEOApiException, CollectionNotFoundException, JobNotFoundException
from openeo_driver.testing import DictSubSet
from openeo_driver.users.oidc import OidcProvider
from .conftest import DEFAULT_MEMOIZER_CONFIG


class TestAggregatorBackendImplementation:

    def test_oidc_providers(self, multi_backend_connection, config, backend1, backend2, requests_mock):
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        providers = implementation.oidc_providers()
        assert providers == [
            OidcProvider(id='egi', issuer='https://egi.test', title='EGI'),
            OidcProvider(id='x-agg', issuer='https://x.test', title='X (agg)'),
            OidcProvider(id='y-agg', issuer='https://y.test', title='Y (agg)'),
            OidcProvider(id='z-agg', issuer='https://z.test', title='Z (agg)'),
        ]

    def test_file_formats_simple(self, multi_backend_connection, config, backend1, backend2, requests_mock):
        just_geotiff = {
            "input": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
            "output": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}}
        }
        requests_mock.get(backend1 + "/file_formats", json=just_geotiff)
        requests_mock.get(backend2 + "/file_formats", json=just_geotiff)
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        file_formats = implementation.file_formats()
        assert file_formats == just_geotiff

    @pytest.mark.parametrize("memoizer_config", [
        DEFAULT_MEMOIZER_CONFIG,
        {"type": "jsondict", "config": {"default_ttl": 66}}  # Test caching with JSON serialization too
    ])
    def test_file_formats_caching(
            self,
            multi_backend_connection, config, backend1, backend2, requests_mock, memoizer_config,
    ):
        just_geotiff = {
            "input": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
            "output": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}}
        }
        mock1 = requests_mock.get(backend1 + "/file_formats", json=just_geotiff)
        mock2 = requests_mock.get(backend2 + "/file_formats", json=just_geotiff)
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        file_formats = implementation.file_formats()
        assert file_formats == just_geotiff
        assert mock1.call_count == 1
        assert mock2.call_count == 1
        _ = implementation.file_formats()
        assert mock1.call_count == 1
        assert mock2.call_count == 1
        with clock_mock(offset=100):
            _ = implementation.file_formats()
        assert mock1.call_count == 2
        assert mock2.call_count == 2

        assert isinstance(implementation._memoizer, DictMemoizer)
        cache_dump = implementation._memoizer.dump(values_only=True)
        assert len(cache_dump) == 1
        expected_type = {"dict": dict, "jsondict": bytes}[memoizer_config["type"]]
        assert all(isinstance(x, expected_type) for x in cache_dump)

    def test_file_formats_merging(self, multi_backend_connection, config, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/file_formats", json={
            "input": {
                "GeoJSON": {"gis_data_types": ["vector"], "parameters": {}}},
            "output": {
                "CSV": {"gis_data_types": ["raster"], "parameters": {}, "title": "Comma Separated Values"},
                "GTiff": {
                    "gis_data_types": ["raster"],
                    "parameters": {"ZLEVEL": {"type": "string", "default": "6"}, },
                    "title": "GeoTiff"
                },
                "JSON": {"gis_data_types": ["raster"], "parameters": {}},
                "netCDF": {"gis_data_types": ["raster"], "parameters": {}, "title": "netCDF"},
            }
        })
        requests_mock.get(backend2 + "/file_formats", json={
            "input": {
                "GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"},
            },
            "output": {
                "GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"},
                "NetCDF": {"gis_data_types": ["raster"], "parameters": {}, "title": "netCDF"},
            }
        })
        implementation = AggregatorBackendImplementation(backends=multi_backend_connection, config=config)
        file_formats = implementation.file_formats()
        assert file_formats == {
            "input": {
                "GeoJSON": {"gis_data_types": ["vector"], "parameters": {}},
                "GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"},
            },
            "output": {
                "CSV": {"gis_data_types": ["raster"], "parameters": {}, "title": "Comma Separated Values"},
                "GTiff": {
                    "gis_data_types": ["raster"],
                    # TODO: merge parameters of backend1 and backend2?
                    "parameters": {"ZLEVEL": {"type": "string", "default": "6"}, },
                    "title": "GeoTiff"
                },
                "JSON": {"gis_data_types": ["raster"], "parameters": {}},
                "netCDF": {"gis_data_types": ["raster"], "parameters": {}, "title": "netCDF"},
            }
        }


class TestInternalCollectionMetadata:

    def test_get_set_backends_for_collection(self):
        internal = _InternalCollectionMetadata()
        internal.set_backends_for_collection("S2", ["b1", "b3"])
        internal.set_backends_for_collection("S1", {"b1": 1, "b2": 2}.keys())
        assert internal.get_backends_for_collection("S2") == ["b1", "b3"]
        assert internal.get_backends_for_collection("S1") == ["b1", "b2"]
        with pytest.raises(CollectionNotFoundException):
            internal.get_backends_for_collection("S2222")

    def test_list_backends_for_collections(self):
        internal = _InternalCollectionMetadata()
        internal.set_backends_for_collection("S2", ["b1", "b3"])
        internal.set_backends_for_collection("S1", ["b1", "b2"])
        assert sorted(internal.list_backends_per_collection()) == [
            ("S1", ["b1", "b2"]),
            ("S2", ["b1", "b3"]),
        ]


@pytest.mark.usefixtures("flask_app")  # Automatically enter flask app context for `url_for` to work
class TestAggregatorCollectionCatalog:

    def test_get_all_metadata_simple(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        metadata = catalog.get_all_metadata()
        assert metadata == [
            {
                'id': 'S2', 'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S2', 'rel': 'self'}
            ]
            }, {
                'id': 'S3', 'links': [
                    {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                    {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                    {'href': 'http://oeoa.test/openeo/1.1.0/collections/S3', 'rel': 'self'}
                ]
            }]

    def test_get_all_metadata_common_collections_minimal(
            self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S3"}, {"id": "S4"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S4"}, {"id": "S5"}]})
        metadata = catalog.get_all_metadata()
        assert metadata == [
            {
                "id": "S3", 'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S3', 'rel': 'self'}]
            },
            {
                "id": "S4", "description": "S4", "title": "S4", "type": "Collection",
                "stac_version": "0.9.0",
                "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[None, None]]}},
                "license": "proprietary",
                "summaries": {"provider:backend": ["b1", "b2"]},
                "links": [
                    {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                    {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                    {'href': 'http://oeoa.test/openeo/1.1.0/collections/S4', 'rel': 'self'}
                ],
            },
            {
                "id": "S5", 'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S5', 'rel': 'self'}]
            },
        ]

    def test_get_all_metadata_common_collections_merging(
            self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{
            "id": "S4",
            "stac_version": "0.9.0",
            "title": "B1's S4", "description": "This is B1's S4",
            "keywords": ["S4", "B1"],
            "version": "1.2.3",
            "license": "MIT",
            "providers": [{"name": "ESA", "roles": ["producer"]}],
            "extent": {
                "spatial": {"bbox": [[-10, 20, 30, 50]]},
                "temporal": {"interval": [["2011-01-01T00:00:00Z", "2019-01-01T00:00:00Z"]]}
            },
            "cube:dimensions": {
                "bands": {"type": "bands", "values": ["B01", "B02"]},
                "x": {"type": "spatial", "axis": "x", "extent": [3, 4]},
            },
            "links": [
                {"rel": "license", "href": "https://spdx.org/licenses/MIT.html"},
            ],
        }]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{
            "id": "S4",
            "stac_version": "0.9.0",
            "title": "B2's S4", "description": "This is B2's S4",
            "keywords": ["S4", "B2"],
            "version": "2.4.6",
            "license": "Apache-1.0",
            "providers": [{"name": "ESA", "roles": ["licensor"]}],
            "extent": {
                "spatial": {"bbox": [[-20, -20, 40, 40]]},
                "temporal": {"interval": [["2012-02-02T00:00:00Z", "2019-01-01T00:00:00Z"]]}
            },
            "cube:dimensions": {
                "bands": {"type": "bands", "values": ["B01", "B02"]},
                "y": {"type": "spatial", "axis": "y"}
            },
            "links": [
                {"rel": "license", "href": "https://spdx.org/licenses/Apache-1.0.html"},
            ],
        }]})
        metadata = catalog.get_all_metadata()
        assert metadata == [
            {
                "id": "S4",
                "title": "B1's S4",
                "description": "This is B1's S4",
                "keywords": ["S4", "B1", "B2"],
                "version": "2.4.6",
                "stac_version": "0.9.0",
                "extent": {
                    "spatial": {"bbox": [[-10, 20, 30, 50], [-20, -20, 40, 40]]},
                    "temporal": {"interval": [["2011-01-01T00:00:00Z", "2019-01-01T00:00:00Z"],
                                              ["2012-02-02T00:00:00Z", "2019-01-01T00:00:00Z"]]}},
                "cube:dimensions": {
                    "bands": {"type": "bands", "values": ["B01", "B02"]},
                    "x": {"type": "spatial", "extent": [3.0, 4.0], "axis": "x"},
                    "y": {"type": "spatial", "axis": "y"}
                },
                "license": "various",
                "providers": [{"name": "ESA", "roles": ["producer"]}, {"name": "ESA", "roles": ["licensor"]}],
                "summaries": {"provider:backend": ["b1", "b2"]},
                "links": [
                    {"rel": "license", "href": "https://spdx.org/licenses/MIT.html"},
                    {"rel": "license", "href": "https://spdx.org/licenses/Apache-1.0.html"},
                    {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "root"},
                    {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "parent"},
                    {"href": "http://oeoa.test/openeo/1.1.0/collections/S4", "rel": "self"},
                ],
                "type": "Collection"
            },
        ]

    def test_get_best_backend_for_collections_basic(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S3"}, {"id": "S4"}]})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S4"}, {"id": "S5"}]})
        with pytest.raises(OpenEOApiException, match="Empty collection set given"):
            catalog.get_backend_candidates_for_collections([])
        assert catalog.get_backend_candidates_for_collections(["S3"]) == ["b1"]
        assert catalog.get_backend_candidates_for_collections(["S4"]) == ["b1", "b2"]
        assert catalog.get_backend_candidates_for_collections(["S5"]) == ["b2"]
        assert catalog.get_backend_candidates_for_collections(["S3", "S4"]) == ["b1"]
        assert catalog.get_backend_candidates_for_collections(["S4", "S5"]) == ["b2"]

        with pytest.raises(OpenEOApiException, match="Collections across multiple backends"):
            catalog.get_backend_candidates_for_collections(["S3", "S4", "S5"])

    def test_get_collection_metadata_basic(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={"id": "S2", "title": "b1's S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S3"}]})
        requests_mock.get(backend2 + "/collections/S3", json={"id": "S3", "title": "b2's S3"})

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            'id': 'S2', 'title': "b1's S2",
            'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S2', 'rel': 'self'},
            ]
        }
        metadata = catalog.get_collection_metadata("S3")
        assert metadata == {
            "id": "S3", "title": "b2's S3",
            'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S3', 'rel': 'self'},
            ]
        }

        with pytest.raises(CollectionNotFoundException):
            catalog.get_collection_metadata("S5")

    def test_get_collection_metadata_merging(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json=
        {
            "id": "S2", "title": "b1's S2",
            "stac_version": "0.9.0",
            "stac_extensions": ["datacube", "sar"],
            "crs": ["http://www.opengis.net/def/crs/OGC/1.3/CRS84", "http://www.opengis.net/def/crs/EPSG/0/2154"],
            "keywords": ["S2", "Sentinel Hub"],
            "version": "1.1.0",
            "license": "special",
            "links": [
                {"rel": "license", "href": "https://special.license.org"},
            ],
            "providers": [{"name": "provider1"}],
            "sci:citation": "Modified Copernicus Sentinel data [Year]/Sentinel Hub",
            "datasource_type": "1sentinel-2-grd",
        })
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json=
        {
            "id": "S2", "title": "b2's S2",
            "stac_version": "1.0.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/datacube/v1.0.0/schema.json",
                "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
                "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
                "https://stac-extensions.github.io/raster/v1.0.0/schema.json"
            ],
            "keywords": ["xcube", "SAR"],
            "version": "0.8.0",
            "deprecated": False,
            "license": "proprietary",
            'links': [
                {"rel": "license", "href": "https://propietary.license"},
            ],
            "providers": [{"name": "provider2"}],
            "datasource_type": "2sentinel-2-grd",
            "sci:citation": "Second citation list."
        })

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "description": "S2",
            "title": "b1's S2",
            "stac_version": "1.0.0",
            "stac_extensions": [
                "datacube",
                "sar",
                "https://stac-extensions.github.io/datacube/v1.0.0/schema.json",
                "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
                "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
                "https://stac-extensions.github.io/raster/v1.0.0/schema.json"
            ],
            "crs": ["http://www.opengis.net/def/crs/OGC/1.3/CRS84", "http://www.opengis.net/def/crs/EPSG/0/2154"],
            "keywords": ["S2", "Sentinel Hub", "xcube", "SAR"],
            "version": "1.1.0",
            "deprecated": False,
            "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[None, None]]}},
            "license": "various",
            "summaries": {"provider:backend": ["b1", "b2"]},
            "links": [
                {"rel": "license", "href": "https://special.license.org"},
                {"rel": "license", "href": "https://propietary.license"},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S2', 'rel': 'self'}
            ],
            "providers": [{"name": "provider1"}, {"name": "provider2"}],
            "type": "Collection",
            "sci:citation": "Modified Copernicus Sentinel data [Year]/Sentinel Hub",
        }

    def test_get_collection_metadata_merging_summaries(
            self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={
            "id": "S2",
            "summaries": {
                "constellation": ["sentinel-1"],
                "instruments": ["c-sar"],
                "platform": ["sentinel-1a", "sentinel-1b"],
                "sar:center_frequency": [5.405],
                "sar:frequency_band": ["C"],
                "sar:instrument_mode": ["SM", "IW", "EW", "WV"],
                "sar:polarizations": ["SH", "SV", "DH", "DV", "HH", "HV", "VV", "VH"],
                "sar:product_type": ["GRD"],
                "sar:resolution": [10, 25, 40],
                "sat:orbit_state": ["ascending", "descending"],
            },
        })
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={
            "id": "S2",
            "summaries": {
                "constellation": ["sentinel-1"],
                "instruments": ["c-sar"],
                "platform": ["sentinel-1"],
                "sar:center_frequency": [5.405],
                "sar:frequency_band": ["C"],
                "sar:instrument_mode": ["IW"],
                "sar:looks_azimuth": [1],
                "sar:looks_equivalent_number": [4.4],
                "sar:looks_range": [5],
                "sar:pixel_spacing_azimuth": [10],
                "sar:pixel_spacing_range": [10],
                "sar:polarizations": ["HH", "VV", "VV+VH", "HH+HV"],
                "sar:product_type": ["GRD"],
                "sar:resolution_azimuth": [22],
                "sar:resolution_range": [20],
            }
        })
        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            'description': 'S2', 'extent': {
                'spatial': {'bbox': [[-180, -90, 180, 90]]}, 'temporal': {'interval': [[None, None]]}
            }, 'id': 'S2', 'license': 'proprietary',
            'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S2', 'rel': 'self'}
            ],
            'summaries': {
                'constellation': ['sentinel-1', 'sentinel-1'], 'instruments': ['c-sar', 'c-sar'],
                'platform': ['sentinel-1a', 'sentinel-1b', 'sentinel-1'], 'provider:backend': ['b1', 'b2'],
                'sar:center_frequency': [5.405, 5.405], 'sar:frequency_band': ['C', 'C'],
                'sar:instrument_mode': ['SM', 'IW', 'EW', 'WV', 'IW'], 'sar:looks_azimuth': [1],
                'sar:looks_equivalent_number': [4.4], 'sar:looks_range': [5], 'sar:pixel_spacing_azimuth': [10],
                'sar:pixel_spacing_range': [10],
                'sar:polarizations': ['SH', 'SV', 'DH', 'DV', 'HH', 'HV', 'VV', 'VH', 'HH', 'VV', 'VV+VH', 'HH+HV'],
                'sar:product_type': ['GRD', 'GRD'], 'sar:resolution': [10, 25, 40], 'sar:resolution_azimuth': [22],
                'sar:resolution_range': [20], 'sat:orbit_state': ['ascending', 'descending']
            }, 'title': 'S2', 'type': 'Collection'
        }
    def test_get_collection_metadata_merging_extent(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={
            "id": "S2", "extent": {
                "spatial": {
                    "bbox": [[-180, -90, 180, 90], [-10, -90, 120, 90]]
                }, "temporal": {
                    "interval": [["2014-10-03T04:14:15Z", None]]
                }
            },
        })
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={
            "id": "S2", "extent": {
                "spatial": {
                    "bbox": [[-180, -90, 180, 90]]
                }, "temporal": {
                    "interval": [["2014-10-03T04:14:15Z", None], [None, None, ]]
                }
            },
        })
        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            'id': 'S2', 'stac_version': '0.9.0', 'title': 'S2', 'description': 'S2', 'type': 'Collection',
            'license': 'proprietary', 'extent': {
                'spatial': {'bbox': [[-180, -90, 180, 90], [-10, -90, 120, 90]]},
                'temporal': {'interval': [['2014-10-03T04:14:15Z', None]]}
            },
            'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S2', 'rel': 'self'},
            ],
            'summaries': {'provider:backend': ['b1', 'b2']}
        }

    def test_get_collection_metadata_merging_links(self, catalog, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={
            "id": "S2", "links": [
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "root"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "parent"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections/S2", "rel": "self"},
                {"href": "http://some.license", "rel": "license"}
            ],
        })
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={
            "id": "S2", "links": [
                {"href": "http://oeoa.test/openeo/1.1.0/collections/S2", "rel": "self"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "parent"},
                {"href": "http://oeoa.test/openeo/1.1.0/collections", "rel": "root"},
            ],
        })

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            'id': 'S2', 'stac_version': '0.9.0', 'title': 'S2', 'description': 'S2', 'type': 'Collection',
            'license': 'proprietary',
            'extent': {
                'spatial': {'bbox': [[-180, -90, 180, 90]]}, 'temporal': {'interval': [[None, None]]}
            },
            'links': [
                {'href': 'http://some.license', 'rel': 'license'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S2', 'rel': 'self'},
            ],
            'summaries': {'provider:backend': ['b1', 'b2']},
        }

    def test_get_collection_metadata_merging_cubedimensions(
            self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={
            "id": "S2", "cube:dimensions": {
                "bands": {
                    "type": "bands", "values": ["VV", "VH", "HV", "HH"]
                }, "t": {
                    "extent": ["2013-10-03T04:14:15Z", "2020-04-03T00:00:00Z"], "step": 1, "type": "temporal"
                }, "x": {
                    "axis": "x", "extent": [-20, 130], "reference_system": {"name": "PROJJSON object."},
                    "step": 10, "type": "spatial"
                }, "y": {
                    "axis": "y", "extent": [-10, 20], "reference_system": {"name": "PROJJSON object."},
                    "step": 10, "type": "spatial"
                }
            }
        })
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={
            "id": "S2", "cube:dimensions": {
                "bands": {
                    "type": "bands", "values": ["VV", "VH", "HH", "HH+HV", "VV+VH", "HV"]
                }, "t": {
                    "extent": ["2013-04-03T00:00:00Z", "2019-04-03T00:00:00Z"], "step": 1, "type": "temporal"
                }, "x": {
                    "axis": "x", "extent": [-40, 120], "type": "spatial",
                    "reference_system": {"name": "PROJJSON object."}
                }, "y": {
                    "axis": "y", "extent": [0, 45], "type": "spatial", "reference_system": {"name": "PROJJSON object."}
                }
            },
        })

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            'id': 'S2',
            'stac_version': '0.9.0',
            'title': 'S2',
            'description': 'S2', 'type': 'Collection', 'license': 'proprietary',
            "cube:dimensions": {
                "bands": {
                    "type": "bands", "values": ["VV", "VH"],
                },
                "t": {
                    "extent": ["2013-04-03T00:00:00Z", "2020-04-03T00:00:00Z"], "step": 1, "type": "temporal"
                }, "x": {
                    "axis": "x", "extent": [-40, 130], "reference_system": {"name": "PROJJSON object."},
                    "step": 10, "type": "spatial"
                }, "y": {
                    "axis": "y", "extent": [-10, 45], "reference_system": {"name": "PROJJSON object."},
                    "step": 10, "type": "spatial"
                }
            },
            'summaries': {'provider:backend': ['b1', 'b2']},
            'extent': {'spatial': {'bbox': [[-180, -90, 180, 90]]}, 'temporal': {'interval': [[None, None]]}},
            'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S2', 'rel': 'self'},
            ]
        }

    @pytest.mark.parametrize(["b1_bands", "b2_bands", "expected_bands"], [
        (["VV", "HH"], ["VV", "HH"], ["VV", "HH"]),
        (["VV", "HH"], ["VV", "HH", "HH+VH"], ["VV", "HH"]),
        (["VV", "HH"], ["VV", "VH"], ["VV"]),
        (["VV", "VH", "HV", "HH"], ["HH", "VV", "HH+HV", "VV+VH"], ["VV", "VH", "HV", "HH"]),
    ])
    def test_get_collection_metadata_merging_bands_prefix(
            self, catalog, backend1, backend2, requests_mock, b1_bands, b2_bands, expected_bands
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", json={
            "id": "S2", "cube:dimensions": {
                "bands": {"type": "bands", "values": b1_bands},
            }
        })
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={
            "id": "S2", "cube:dimensions": {
                "bands": {"type": "bands", "values": b2_bands},
            },
        })

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            'id': 'S2',
            'stac_version': '0.9.0',
            'title': 'S2',
            'description': 'S2', 'type': 'Collection', 'license': 'proprietary',
            "cube:dimensions": {
                "bands": {"type": "bands", "values": expected_bands, },
            },
            'summaries': {'provider:backend': ['b1', 'b2']},
            'extent': {'spatial': {'bbox': [[-180, -90, 180, 90]]}, 'temporal': {'interval': [[None, None]]}},
            'links': [
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'root'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections', 'rel': 'parent'},
                {'href': 'http://oeoa.test/openeo/1.1.0/collections/S2', 'rel': 'self'},
            ]
        }

    def test_get_collection_metadata_merging_with_error(
            self, catalog, backend1, backend2, requests_mock
    ):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend1 + "/collections/S2", status_code=500)
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        requests_mock.get(backend2 + "/collections/S2", json={"id": "S2", "title": "b2's S2"})

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == {
            "id": "S2",
            "title": "b2's S2",
            'links': [{
                'href': 'http://oeoa.test/openeo/1.1.0/collections',
                'rel': 'root'
            }, {
                'href': 'http://oeoa.test/openeo/1.1.0/collections',
                'rel': 'parent'
            }, {
                'href': 'http://oeoa.test/openeo/1.1.0/collections/S2',
                'rel': 'self'
            }]
        }
        # TODO: test that caching of result is different from merging without error? (#2)

    # TODO tests for caching of collection metadata

    def test_generate_backend_constraint_callables(self):
        callables = AggregatorCollectionCatalog.generate_backend_constraint_callables([
            {"eq": {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "b1"}, "result": True}},
            {"eq": {"process_id": "neq", "arguments": {"x": {"from_parameter": "value"}, "y": "b2"}, "result": True}},
        ])
        equals_b1, differs_from_b2 = callables
        assert equals_b1("b1") is True
        assert equals_b1("b2") is False
        assert equals_b1("b3") is False
        assert differs_from_b2("b1") is True
        assert differs_from_b2("b2") is False
        assert differs_from_b2("b3") is True

    @pytest.mark.parametrize("memoizer_config", [
        DEFAULT_MEMOIZER_CONFIG,
        {"type": "jsondict", "config": {"default_ttl": 66}}  # Test caching with JSON serialization too
    ])
    def test_get_all_metadata_caching(self, catalog, backend1, backend2, requests_mock, memoizer_config):
        b1am = requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        b2am = requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})

        metadata = catalog.get_all_metadata()
        assert metadata == [DictSubSet({"id": "S2"})]
        assert (b1am.call_count, b2am.call_count) == (1, 1)

        with clock_mock(offset=10):
            metadata = catalog.get_all_metadata()
            assert metadata == [DictSubSet({"id": "S2"})]
            assert (b1am.call_count, b2am.call_count) == (1, 1)

        with clock_mock(offset=100):
            metadata = catalog.get_all_metadata()
            assert metadata == [DictSubSet({"id": "S2"})]
            assert (b1am.call_count, b2am.call_count) == (2, 2)

    @pytest.mark.parametrize("memoizer_config", [
        DEFAULT_MEMOIZER_CONFIG,
        {"type": "jsondict", "config": {"default_ttl": 66}}  # Test caching with JSON serialization too
    ])
    def test_get_collection_metadata_caching(self, catalog, backend1, backend2, requests_mock, memoizer_config):
        requests_mock.get(backend1 + "/collections", json={"collections": [{"id": "S2"}]})
        b1s2 = requests_mock.get(backend1 + "/collections/S2", json={"id": "S2", "title": "b1's S2"})
        requests_mock.get(backend2 + "/collections", json={"collections": [{"id": "S2"}]})
        b2s2 = requests_mock.get(backend2 + "/collections/S2", json={"id": "S2", "title": "b2's S2"})

        metadata = catalog.get_collection_metadata("S2")
        assert metadata == DictSubSet({'id': 'S2', 'title': "b1's S2"})
        assert (b1s2.call_count, b2s2.call_count) == (1, 1)

        with clock_mock(offset=10):
            metadata = catalog.get_collection_metadata("S2")
            assert metadata == DictSubSet({'id': 'S2', 'title': "b1's S2"})
            assert (b1s2.call_count, b2s2.call_count) == (1, 1)

        with clock_mock(offset=100):
            metadata = catalog.get_collection_metadata("S2")
            assert metadata == DictSubSet({'id': 'S2', 'title': "b1's S2"})
            assert (b1s2.call_count, b2s2.call_count) == (2, 2)


class TestJobIdMapping:

    def test_get_aggregator_job_id(self):
        assert JobIdMapping.get_aggregator_job_id(
            backend_job_id="j0bId-f00b6r", backend_id="vito"
        ) == "vito-j0bId-f00b6r"

    def test_parse_aggregator_job_id(self, multi_backend_connection):
        assert JobIdMapping.parse_aggregator_job_id(
            backends=multi_backend_connection, aggregator_job_id="b1-j000b"
        ) == ("j000b", "b1")
        assert JobIdMapping.parse_aggregator_job_id(
            backends=multi_backend_connection, aggregator_job_id="b2-b6tch-j0b-o123423"
        ) == ("b6tch-j0b-o123423", "b2")

    def test_parse_aggregator_job_id_fail(self, multi_backend_connection):
        with pytest.raises(JobNotFoundException):
            JobIdMapping.parse_aggregator_job_id(
                backends=multi_backend_connection, aggregator_job_id="b3-b6tch-j0b-o123423"
            )


class TestAggregatorProcessing:

    def test_get_process_registry(self, catalog, multi_backend_connection, config, backend1, backend2, requests_mock):
        requests_mock.get(backend1 + "/processes", json={"processes": [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]})
        requests_mock.get(backend2 + "/processes", json={"processes": [
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
        ]})
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog, config=config)
        registry = processing.get_process_registry(api_version="1.0.0")
        assert sorted(registry.get_specs(), key=lambda p: p["id"]) == [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "data"}]},
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
        ]

    @pytest.mark.parametrize("memoizer_config", [
        DEFAULT_MEMOIZER_CONFIG,
        {"type": "jsondict", "config": {"default_ttl": 66}}  # Test caching with JSON serialization too
    ])
    def test_get_process_registry_caching(
            self, catalog, multi_backend_connection, config, backend1, backend2, requests_mock,
            memoizer_config
    ):
        b1p = requests_mock.get(backend1 + "/processes", json={"processes": [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
        ]})
        b2p = requests_mock.get(backend2 + "/processes", json={"processes": [
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
        ]})
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog, config=config)
        assert (b1p.call_count, b2p.call_count) == (0, 0)

        _ = processing.get_process_registry(api_version="1.0.0")
        assert (b1p.call_count, b2p.call_count) == (1, 1)

        with clock_mock(offset=10):
            _ = processing.get_process_registry(api_version="1.0.0")
            assert (b1p.call_count, b2p.call_count) == (1, 1)

        with clock_mock(offset=1000):
            _ = processing.get_process_registry(api_version="1.0.0")
            assert (b1p.call_count, b2p.call_count) == (2, 2)

    def test_get_process_registry_parameter_differences(
            self, catalog, multi_backend_connection, config, backend1, backend2,
            requests_mock,
    ):
        requests_mock.get(backend1 + "/processes", json={"processes": [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "array"}]},
        ]})
        requests_mock.get(backend2 + "/processes", json={"processes": [
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "values"}]},
        ]})
        processing = AggregatorProcessing(backends=multi_backend_connection, catalog=catalog, config=config)
        registry = processing.get_process_registry(api_version="1.0.0")
        assert sorted(registry.get_specs(), key=lambda p: p["id"]) == [
            {"id": "add", "parameters": [{"name": "x"}, {"name": "y"}]},
            {"id": "mean", "parameters": [{"name": "array"}]},
            {"id": "multiply", "parameters": [{"name": "x"}, {"name": "y"}]},
        ]

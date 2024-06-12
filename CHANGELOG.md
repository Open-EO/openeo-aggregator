# Changelog

All notable changes to this project will be documented in this file.

The format is roughly based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

<!-- start changelog -->

## 0.35.0

- Start with `openeo-aggregator` docs, hosted with GitHub Pages at https://open-eo.github.io/openeo-aggregator/ ([#142](https://github.com/Open-EO/openeo-aggregator/issues/142))

## 0.34.0

- Also support `job_options_update` to inject job options in synchronous processing requests ([#135](https://github.com/Open-EO/openeo-aggregator/issues/135), eu-cdse/openeo-cdse-infra#114)

## 0.33.0

- Pass through original API errors on synchronous or batch processing ([#121](https://github.com/Open-EO/openeo-aggregator/issues/121))

## 0.32.0

- Add config option `process_allowed` to include/exclude processes ([#137](https://github.com/Open-EO/openeo-aggregator/issues/137))

## [0.31.0]

- Remove deprecated `collection_whitelist` config ([#139](https://github.com/Open-EO/openeo-aggregator/issues/139))

## [0.30.0]

- Add backend-aware collection allow-list option ([#139](https://github.com/Open-EO/openeo-aggregator/issues/139))

## [0.29.0]

- Add config option `job_options_update` to inject job options before sending a job to upstream back-end ([#135](https://github.com/Open-EO/openeo-aggregator/issues/135))

## [0.28.0]

- Remove (now unused) `AggregatorConfig` class definition ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.27.0]

- Remove deprecated `AggregatorConfig` fields: `aggregator_backends`, `partitioned_job_tracking`,
  `zookeeper_prefix`, `memoizer` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))
- Remove `AggregatorConfig` usage (both in `src` and `tests`). Remove support for `OPENEO_AGGREGATOR_CONFIG` based configuration ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.26.0]

- Remove now unused `conf/backend_config.py` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112), [#117](https://github.com/Open-EO/openeo-aggregator/issues/117))

## [0.25.0]

- Add `AggregatorBackendConfig.memoizer` and deprecate `AggregatorConfig.memoizer` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.24.0]

- Add `AggregatorBackendConfig.partitioned_job_tracking` and deprecate `AggregatorConfig.partitioned_job_tracking` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.23.0]

- Add `AggregatorBackendConfig.aggregator_backends` and deprecate `AggregatorConfig.aggregator_backends` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.22.0]

- Eliminate unused `AggregatorConfig.configured_oidc_providers` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.21.0]

- Add `AggregatorBackendConfig` to `conf/aggregator.*.py` files  ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.20.0]

- move `connections_cache_ttl` config to AggregatorBackendConfig ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))
- replace `kazoo_client_factory` config with `zk_memoizer_tracking` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))
- Add `AggregatorBackendConfig.zookeeper_prefix` and deprecate `AggregatorConfig.zookeeper_prefix` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.19.0]

- Support regexes in `collection_whitelist` config (eu-cdse/openeo-cdse-infra#54)

## [0.18.4]

- Move `collection_whitelist` config to `AggregatorBackendConfig` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.18.3]

- Move `auth_entitlement_check` config to `AggregatorBackendConfig`  ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.18.2]

- Add support for standard `OpenEoBackendConfig.oidc_providers` and deprecate `AggregatorConfig.configured_oidc_providers` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))

## [0.18.1]

- Include gunicorn configs in wheel (related to [#117](https://github.com/Open-EO/openeo-aggregator/issues/117))

## [0.18.0]

- Start porting `AggregatorConfig` fields to `AggregatorBackendConfig` ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))


## [0.17.0]

- Support `aggregator_config` as `AggregatorConfig` variable name to simplify config system migration
  (define `AggregatorConfig` and `AggregatorBackendConfig` in same config file)
  ([#112](https://github.com/Open-EO/openeo-aggregator/issues/112))


## [0.16.x]

- Disable `auth_entitlement_check` (check on EGI VO entitlements) in all configs ([#133](https://github.com/Open-EO/openeo-aggregator/issues/133))

## [0.15.x]

- Basic support for `POST /validation`: pass through validation of best upstream backend for given process graph ([#42](https://github.com/Open-EO/openeo-aggregator/issues/42))


## [0.14.x]

- Disassociate billing plans from user roles and don't list any for now ([openEOPlatform/architecture-docs#381](https://github.com/openEOPlatform/architecture-docs/issues/381))


## [0.13.x]

- Fix compatibility with `openeo_driver>=0.75.0` (new `enable_basic_auth` config, which is going to be disabled by default)


## [0.12.x]

- Add (optional) config for collection id whitelisting.
  Keep union of all "upstream" collections as default.
  ([#129](https://github.com/Open-EO/openeo-aggregator/issues/129))
- Disable `auto_validation` feature of latest `openeo` python client library release ([#130](https://github.com/Open-EO/openeo-aggregator/issues/130))
- Fix compatibility with `openeo>=0.25.0` (introduction of `OpenEoApiPlainError`)


## [0.11.x]

- Dockerfile: switch to `python:3.9-slim-bullseye` base image
- Parallelize `/jobs` requests to upstream back-ends ([#28](https://github.com/Open-EO/openeo-aggregator/issues/28))
- Increase timeout on getting batch job logs to 120s ([#128](https://github.com/Open-EO/openeo-aggregator/issues/128))


## [0.10.x]

### Added

- Added simple UDP support by directly proxying to "first" upstream backend (e.g. VITO)
  ([#90](https://github.com/Open-EO/openeo-aggregator/issues/90))
- Background task to prime caches ([#74](https://github.com/Open-EO/openeo-aggregator/issues/74))

### Removed

- Removed `ENV` based config loading for clarity, `OPENEO_AGGREGATOR_CONFIG` must be full explicit path to config ([#117](https://github.com/Open-EO/openeo-aggregator/issues/117))


## [0.9.x]

### Added

- Initial aggregator-level implementation of cross-backend processing
  ([#115](https://github.com/Open-EO/openeo-aggregator/issues/115))


## [0.8.x]

### Added

- Initial, experimental (client-side) proof of concept for cross-backend processing
  ([#95](https://github.com/Open-EO/openeo-aggregator/issues/95))
- Consider process availability in backend selection
  ([#100](https://github.com/Open-EO/openeo-aggregator/issues/100))
- Relax openEO API version constraints for upstream back-ends
  ([#103](https://github.com/Open-EO/openeo-aggregator/issues/103))


### Changed
- Support log level for retrieving BatchJob logs
  ([#106](https://github.com/Open-EO/openeo-aggregator/issues/106))

### Fixed

- User job listing: skip upstream jobs that trigger parse issues instead of failing the whole listing
  ([#109](https://github.com/Open-EO/openeo-aggregator/issues/109))
- User job listing: support dates with fractional dates
  ([#109](https://github.com/Open-EO/openeo-aggregator/issues/109))


## [0.7.x]

### Added

- Add support for more user roles ([#64](https://github.com/Open-EO/openeo-aggregator/issues/64))
- Experimental: allow back-end selection through job option
- Add support for `load_result` with a URL (instead of job_id).
  ([#95](https://github.com/Open-EO/openeo-aggregator/issues/95))
- Batch job result metadata: preserve upstream's canonical link
  ([#98](https://github.com/Open-EO/openeo-aggregator/issues/98))

### Changed

- Change billing currency from EUR to credits ([#96](https://github.com/Open-EO/openeo-aggregator/issues/96))

### Fixed

- Merging of collection metadata produced duplicate entries in `links`: [openEOPlatform/architecture-docs#266](https://github.com/openEOPlatform/architecture-docs/issues/266)

## [0.6.x]

### Added

- Add SentinelHub openEO backend to production config
- Implement support for federation of secondary services
  ([#78](https://github.com/Open-EO/openeo-aggregator/issues/78)).
  Including:
  caching
  ([#84](https://github.com/Open-EO/openeo-aggregator/issues/84))
  and skipping of that do not support secondary services
  ([#85](https://github.com/Open-EO/openeo-aggregator/issues/85))
- Start using pre-commit and black/darker for code style consistency

### Changed

- Refactored/improved metadata merging/validation of collections and processes
  ([#76](https://github.com/Open-EO/openeo-aggregator/issues/76),
  [#77](https://github.com/Open-EO/openeo-aggregator/issues/77))


## [0.5.x]

### Changed

- Change to openEO API 1.1.0 version of terrascope/vito backend
- Make sure user id (prefix) is logged in JSON logs
- Updated (generous fallback) "FreeTier" user role to 30DayTrial (more strict)
- Use EODC dev instance in aggregator dev config
- Update EGI issuer URL to new Keycloak one (keep old provider under "egi-legacy")
- Improve collection metadata merging ([#5](https://github.com/Open-EO/openeo-aggregator/issues/5))
- Replace local memory cache with centralized ZooKeeper based cache for metadata documents ([#2](https://github.com/Open-EO/openeo-aggregator/issues/2))
- Fix issue https://discuss.eodc.eu/t/invalid-band-name-index-vv/472

### Fixed

- Properly rewrite model id in `load_ml_model` ([#70](https://github.com/Open-EO/openeo-aggregator/issues/70))

## [0.4.x]

### Added

- Initial implementation of "partitioned" job management, e.g. for large area processing (EP-3475, [openEOPlatform/architecture-docs#12](https://github.com/openEOPlatform/architecture-docs/issues/12))

### Changed

- Update to the latest version of openeo-driver API

### Fixed

- Fixed stripping back-end id prefix in `load_result` calls in batch jobs ([#19](https://github.com/Open-EO/openeo-aggregator/issues/19))
- Fixed missing "cube:dimension" property on merged collections ([openEOPlatform/SRR1_notebooks#9](https://github.com/openEOPlatform/SRR1_notebooks/issues/9))
- Use default connection timeout on openEO version discovery request too


## [0.3.x]

### Added

- Expose openEO Platform billing plans ([#6](https://github.com/Open-EO/openeo-aggregator/issues/6))
- Set `default_plan` on `/me` for early adopters ([#6](https://github.com/Open-EO/openeo-aggregator/issues/6))
- Allow access to "free tier" users (users enrolled in openEO Platform virtual org, but without the "early adopter" role) ([openEOPlatform/architecture-docs#169](https://github.com/openEOPlatform/architecture-docs/issues/169) / EP-4092)
- Add (experimental) "federation:missing" on partial user job listings ([#27](https://github.com/Open-EO/openeo-aggregator/issues/27))
- Add CREODIAS back-end on dev instance
- Automatically do warning logs on "slow" backend responses
- Add request correlation id to (JSON) logging

### Changed

- Refresh back-end connection objects regularly instead of holding on the same ones.
  Improves resilience of aggregator when a back-end is down. ([#18](https://github.com/Open-EO/openeo-aggregator/issues/18) / EP-4049)
- Fine-tune EGI Early Adopter related "entitlement" error messages ([openEOPlatform/architecture-docs#105](https://github.com/openEOPlatform/architecture-docs/issues/105))
- Do logging in JSON format, targeting ElasticSearch based monitoring (EP-4057)
- More aggressive and harmonized exception handling/logging ([#18](https://github.com/Open-EO/openeo-aggregator/issues/18) / EP-4049)
- Enable `DEBUG` logging for `openeo_aggregator` logs (EP-4057)

### Fixed

- Preserve "job_options" field in batch job submit
- Preserve "usage" batch job metadata ([#31](https://github.com/Open-EO/openeo-aggregator/issues/31))



## [0.2.x]

### Added

- Added support for `/collections/{cid}/items` (EP-4022, [openEOPlatform/architecture-docs#104](https://github.com/openEOPlatform/architecture-docs/issues/104))
- Implement `/health` endpoint for monitoring (EP-3906)
- Initial implementation to list federation backends in capabilities doc ([#22](https://github.com/Open-EO/openeo-aggregator/issues/22))

### Changed

- Access to authenticated endpoints requires the "early adopter" role now (through EGI Check-in `eduPersonEntitlement`) (EP-3969)
- Work with aggregator-specific OIDC provider settings (e.g. dedicated default clients) (EP-4046, [#7](https://github.com/Open-EO/openeo-aggregator/issues/7))
- Disable `egi-dev` OIDC provider (EP-4046, [#7](https://github.com/Open-EO/openeo-aggregator/issues/7))
- Improve "early adopter check" error messages ([openEOPlatform/architecture-docs#105](https://github.com/openEOPlatform/architecture-docs/issues/105))
- Only allow OIDC authentication in production (no basic auth)
- User email address as user name in `/me`
- Config loading: replace JSON based configs with Python based config files
- Move aggregator configs to dedicated dev/prod config files

### Fixed

- Use only one thread per gunicorn worker to avoid race conditions and thread safety issues ([#13](https://github.com/Open-EO/openeo-aggregator/issues/13))
- Increase gunicorn worker timeout to handle long (sync) requests better ([#15](https://github.com/Open-EO/openeo-aggregator/issues/15))
- support aggregator job id mapping in `load_result` ([#19](https://github.com/Open-EO/openeo-aggregator/issues/19))



## [0.1.x]

### Added
- Initial support for user selected backend through `load_collection` properties filtering (EP-4011, [openEOPlatform/architecture-docs#85](https://github.com/openEOPlatform/architecture-docs/issues/85))


## [0.0.3]

### Added

- Add `/file_formats` support ([#1](https://github.com/Open-EO/openeo-aggregator/issues/1))
- Add initial support for multi-backend collection metadata merging (EP-4011, [openEOPlatform/architecture-docs#85](https://github.com/openEOPlatform/architecture-docs/issues/85))

### Changed

- Increase default cache TTL to 6 hours ([#2](https://github.com/Open-EO/openeo-aggregator/issues/2))
- Disable CREODIAS based backend (too unstable at the moment)
- Take union of processes instead of intersection ([#4](https://github.com/Open-EO/openeo-aggregator/issues/4))
- Increase timeout for job create/start to 5 minutes (EP-4021)
- Switch to production EODC instance
- Increase default connection timeout from 20s to 30s

### Fixed

- Skip failing back-ends when merging `/jobs` listings (EP-4014)


<!-- end changelog -->

# Changelog

All notable changes to this project will be documented in this file.

The format is roughly based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Current: 0.7.x]

### Added

- Add support for more user roles ([#64](https://github.com/Open-EO/openeo-aggregator/issues/64))

### Changed

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

# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).



## [Unreleased: 0.1.0a1]

### Added

### Changed

### Fixed



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

# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Removed

### Fixed

## [0.2.0] - 2024-10-10

### Added
- Added support for `FetchType.POINT` in Sentinel-1 orbit state selection
- Added CPU, max-executor-memory and duration metadata in job tracker
- Generic fetchers now accept STAC collections
- Added `split_collection_by_epsg` helper function
### Changed
- Job splitters now return original geometries instead of the centroids
### Removed

### Fixed
- CRS warnings removed from `split_job_s2grid`

## [0.1.0] - 2024-06-21

Initial Build



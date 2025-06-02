# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Removed

### Fixed

## [0.4.6] - 2025-06-02

### Added
- Users can now specify UDF dependencies for the `FeatureExtractor` class via job options, using the `feature_deps` name.
### Changed

### Removed

### Fixed

## [0.4.5] - 2025-05-09

### Added

### Changed
- `apply_model_inference` now uses `apply_metadata` inside the inference UDF, instead of `rename_labels` to change the band names
### Removed

### Fixed

## [0.4.4] - 2025-04-28

### Added
- `FetchType.POINT` and `FetchType.POLYGON` no longer require a spatial_extent
### Changed

### Removed

### Fixed

## [0.4.3] - 2025-03-26

### Added
`decompress_backscatter_uint16`
### Changed

### Removed

### Fixed

## [0.4.2] - 2025-03-06

### Added

### Changed
- The `spatial_extent` argument is no longer given to `load_collection` for `FetchType.POINT`
### Removed

### Fixed

## [0.4.1] - 2025-02-21

### Added

### Changed

### Removed

### Fixed
Corrected openeofed backend URL.

## [0.4.0] - 2025-01-31

### Added

### Changed
- `split_job_s2grid` now uses a non-overlapping S2 grid to determine the S2 tile ID of a geometry
### Removed

### Fixed
 
## [0.3.0] - 2025-01-08

### Added
- `split_job_s2sphere` function to split jobs into S2cells of the s2sphere package. More info: http://s2geometry.io/. This job splitter recursively splits cells until the number of points in each cell is less than a given threshold.

### Changed
- `ouput_path_generator` in `GFMapJobManager.on_job_done` now requires `asset_id` as a keyword argument
### Removed

### Fixed
- Fixed bug where `s1_area_per_orbitstate_vvvh` failed for FeatureCollections containing a single point
- Fixed bug where gfmap didn't work with the latest version of the openeo-python-client
- Fixed centroid calculation of S2 grid in `split_job_s2grid`
- Fixed wrong nodata values in `compress_backscatter_uint16` for backend CDSE, CDSE_STAGING and FED

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



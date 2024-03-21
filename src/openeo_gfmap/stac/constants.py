"""
Constants in the STAC collection generated after a series of batch jobs
"""
import pystac

LICENSE = "CC-BY-4.0"
LICENSE_LINK = pystac.Link(
    rel="license",
    target="https://spdx.org/licenses/CC-BY-4.0.html",
    media_type=pystac.MediaType.HTML,
    title="Creative Commons Attribution 4.0 International License",
)
STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
    "https://stac-extensions.github.io/file/v2.1.0/schema.json",
    "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
    "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
]
CONSTELLATION = ["sentinel-2"]
PLATFORM = ["sentinel-2a", "sentinel-2b"]
INSTRUMENTS = ["msi"]
GSD = [10, 20, 60]
SUMMARIES = pystac.Summaries(
    {
        "constellation": CONSTELLATION,
        "platform": PLATFORM,
        "instruments": INSTRUMENTS,
        "gsd": GSD,
    }
)


def create_spatial_dimension(
    name: str,
) -> pystac.extensions.datacube.HorizontalSpatialDimension:
    return pystac.extensions.datacube.HorizontalSpatialDimension(
        {
            "axis": name,
            "step": 10,
            "reference_system": {
                "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
                "area": "World",
                "bbox": {
                    "east_longitude": 180,
                    "north_latitude": 90,
                    "south_latitude": -90,
                    "west_longitude": -180,
                },
                "coordinate_system": {
                    "axis": [
                        {
                            "abbreviation": "Lat",
                            "direction": "north",
                            "name": "Geodetic latitude",
                            "unit": "degree",
                        },
                        {
                            "abbreviation": "Lon",
                            "direction": "east",
                            "name": "Geodetic longitude",
                            "unit": "degree",
                        },
                    ],
                    "subtype": "ellipsoidal",
                },
                "datum": {
                    "ellipsoid": {
                        "inverse_flattening": 298.257223563,
                        "name": "WGS 84",
                        "semi_major_axis": 6378137,
                    },
                    "name": "World Geodetic System 1984",
                    "type": "GeodeticReferenceFrame",
                },
                "id": {"authority": "OGC", "code": "Auto42001", "version": "1.3"},
                "name": "AUTO 42001 (Universal Transverse Mercator)",
                "type": "GeodeticCRS",
            },
        }
    )


TEMPORAL_DIMENSION = pystac.extensions.datacube.TemporalDimension(
    {"extent": ["2015-06-23T00:00:00Z", "2019-07-10T13:44:56Z"], "step": "P5D"}
)

BANDS_DIMENSION = pystac.extensions.datacube.AdditionalDimension(
    {
        "values": [
            "S2-L2A-SCL",
            "S2-L2A-B01",
            "S2-L2A-B02",
            "S2-L2A-B03",
            "S2-L2A-B04",
            "S2-L2A-B05",
            "S2-L2A-B06",
            "S2-L2A-B07",
            "S2-L2A-B08",
            "S2-L2A-B8A",
            "S2-L2A-B09",
            "S2-L2A-B10",
            "S2-L2A-B11",
            "S2-L2A-B12",
            "LABEL",
        ]
    }
)

CUBE_DIMENSIONS = {
    "x": create_spatial_dimension("x"),
    "y": create_spatial_dimension("y"),
    "time": TEMPORAL_DIMENSION,
    "spectral": BANDS_DIMENSION,
}

SENTINEL2 = pystac.extensions.item_assets.AssetDefinition(
    {
        "gsd": 10,
        "title": "Sentinel2",
        "description": "Sentinel-2 bands",
        "type": "application/x-netcdf",
        "roles": ["data"],
        "proj:shape": [64, 64],
        "raster:bands": [{"name": "S2-L2A-B01"}, {"name": "S2-L2A-B02"}],
        "cube:variables": {
            "S2-L2A-B01": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-B02": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-B03": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-B04": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-B05": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-B06": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-B07": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-B8A": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-B08": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-B11": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-B12": {"dimensions": ["time", "y", "x"], "type": "data"},
            "S2-L2A-SCL": {"dimensions": ["time", "y", "x"], "type": "data"},
        },
        "eo:bands": [
            {
                "name": "S2-L2A-B01",
                "common_name": "coastal",
                "center_wavelength": 0.443,
                "full_width_half_max": 0.027,
            },
            {
                "name": "S2-L2A-B02",
                "common_name": "blue",
                "center_wavelength": 0.49,
                "full_width_half_max": 0.098,
            },
            {
                "name": "S2-L2A-B03",
                "common_name": "green",
                "center_wavelength": 0.56,
                "full_width_half_max": 0.045,
            },
            {
                "name": "S2-L2A-B04",
                "common_name": "red",
                "center_wavelength": 0.665,
                "full_width_half_max": 0.038,
            },
            {
                "name": "S2-L2A-B05",
                "common_name": "rededge",
                "center_wavelength": 0.704,
                "full_width_half_max": 0.019,
            },
            {
                "name": "S2-L2A-B06",
                "common_name": "rededge",
                "center_wavelength": 0.74,
                "full_width_half_max": 0.018,
            },
            {
                "name": "S2-L2A-B07",
                "common_name": "rededge",
                "center_wavelength": 0.783,
                "full_width_half_max": 0.028,
            },
            {
                "name": "S2-L2A-B08",
                "common_name": "nir",
                "center_wavelength": 0.842,
                "full_width_half_max": 0.145,
            },
            {
                "name": "S2-L2A-B8A",
                "common_name": "nir08",
                "center_wavelength": 0.865,
                "full_width_half_max": 0.033,
            },
            {
                "name": "S2-L2A-B11",
                "common_name": "swir16",
                "center_wavelength": 1.61,
                "full_width_half_max": 0.143,
            },
            {
                "name": "S2-L2A-B12",
                "common_name": "swir22",
                "center_wavelength": 2.19,
                "full_width_half_max": 0.242,
            },
        ],
    }
)

SENTINEL1 = pystac.extensions.item_assets.AssetDefinition({})

AGERA5 = pystac.extensions.item_assets.AssetDefinition({})

ITEM_ASSETS = {
    "sentinel2": SENTINEL2,
    # "auxiliary": AUXILIARY,
    "sentinel1": SENTINEL1,
    "agera5": AGERA5,
}

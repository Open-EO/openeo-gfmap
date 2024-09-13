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
CONSTELLATION = {
    "sentinel2": ["sentinel-2"],
    "sentinel1": ["sentinel-1"],
}

PLATFORM = {
    "sentinel2": ["sentinel-2a", "sentinel-2b"],
    "sentinel1": ["sentinel-1a", "sentinel-1b"],
}

INSTRUMENTS = {"sentinel2": ["msi"], "sentinel1": ["c-sar"]}

GSD = {"sentinel2": [10, 20, 60], "sentinel1": [20]}

SUMMARIES = {
    "sentinel2": pystac.summaries.Summaries(
        {
            "constellation": CONSTELLATION["sentinel2"],
            "platform": PLATFORM["sentinel2"],
            "instruments": INSTRUMENTS["sentinel2"],
            "gsd": GSD["sentinel2"],
        }
    ),
    "sentinel1": pystac.summaries.Summaries(
        {
            "constellation": CONSTELLATION["sentinel1"],
            "platform": PLATFORM["sentinel1"],
            "instruments": INSTRUMENTS["sentinel1"],
            "gsd": GSD["sentinel1"],
        }
    ),
}

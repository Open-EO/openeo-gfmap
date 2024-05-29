"""Metadata utilities related to the usage of a DataCube. Used to interract
with the OpenEO backends and cover some shortcomings.
"""

from dataclasses import dataclass


@dataclass
class FakeMetadata:
    """Fake metdata object used for datacubes fetched from STAC catalogues.
    This is used as a temporal fix for OpenEO backend shortcomings, but
    will become unused with the time.
    """

    band_names: list

    def rename_labels(self, _, target, source):
        """Rename the labels of the band dimension."""
        mapping = dict(zip(target, source))
        band_names = self.band_names.copy()
        for idx, name in enumerate(band_names):
            if name in target:
                self.band_names[idx] = mapping[name]
        return self

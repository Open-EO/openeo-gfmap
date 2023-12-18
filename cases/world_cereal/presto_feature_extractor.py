import xarray as xr

from openeo_gfmap.features import PatchFeatureExtractor


class PrestoFeatureExtractor(PatchFeatureExtractor):
    def compute_months(self, inarr: xr.DataArray):
        pass

    def compute(self, inarr: xr.DataArray):
        latlon = self.get_latlons(inarr)


if __name__ == "__main__":
    from openeo_gfmap.features.feature_extractor import apply_feature_extractor

    print(apply_feature_extractor(PrestoFeatureExtractor, None, None, None, None))

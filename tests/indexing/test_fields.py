from splitgill.indexing.fields import geo_path, RootField


class TestGeoPath:
    def test_without_radius(self):
        assert geo_path("lat", "lon") == f"{RootField.GEO}.lat/lon"

    def test_with_radius(self):
        assert geo_path("lat", "lon", "rad") == f"{RootField.GEO}.lat/lon/rad"

"""Geographic regions the probe subscribes to and classifies observations into."""

from dataclasses import dataclass

UNCLASSIFIED = "unclassified"


@dataclass(frozen=True, slots=True)
class Region:
    name: str
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    is_control: bool = False

    def contains(self, lat: float, lon: float) -> bool:
        return self.lat_min <= lat <= self.lat_max and self.lon_min <= lon <= self.lon_max

    def bounding_box(self) -> list[list[float]]:
        """AISStream box: two [latitude, longitude] corners."""
        return [[self.lat_min, self.lon_min], [self.lat_max, self.lon_max]]


# Target boxes deliberately include port approaches, where shore receivers are most likely.
HORMUZ = Region("hormuz", 24.5, 27.5, 54.5, 58.0)
BAB_EL_MANDEB = Region("bab_el_mandeb", 11.0, 14.5, 41.5, 45.5)
# Rotterdam approaches: dense terrestrial coverage, used only to show the feed is delivering.
CONTROL = Region("control", 51.8, 52.1, 3.8, 4.6, is_control=True)

TARGET_REGIONS: dict[str, Region] = {r.name: r for r in (HORMUZ, BAB_EL_MANDEB)}


def classify(lat: float, lon: float, regions: list[Region]) -> str | None:
    """Name of the first region containing the point, or None."""
    for region in regions:
        if region.contains(lat, lon):
            return region.name
    return None

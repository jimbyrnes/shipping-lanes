import itertools

import pytest

from aisprobe.regions import BAB_EL_MANDEB, CONTROL, HORMUZ, classify

ALL = [HORMUZ, BAB_EL_MANDEB, CONTROL]


@pytest.mark.parametrize(
    ("lat", "lon", "expected"),
    [
        (26.57, 56.25, "hormuz"),  # narrowest part of the strait
        (25.10, 56.40, "hormuz"),  # Fujairah anchorage
        (12.60, 43.33, "bab_el_mandeb"),  # the strait
        (11.60, 43.15, "bab_el_mandeb"),  # Djibouti
        (51.95, 4.10, "control"),  # Maasvlakte
        (1.25, 103.80, None),  # Singapore
        (56.25, 26.57, None),  # Hormuz with lat/lon swapped
        (43.33, 12.60, None),  # Bab el-Mandeb with lat/lon swapped
    ],
)
def test_classify(lat, lon, expected):
    assert classify(lat, lon, ALL) == expected


def test_edges_are_inclusive():
    assert HORMUZ.contains(HORMUZ.lat_min, HORMUZ.lon_min)
    assert HORMUZ.contains(HORMUZ.lat_max, HORMUZ.lon_max)
    assert not HORMUZ.contains(HORMUZ.lat_max + 0.001, HORMUZ.lon_max)
    assert not HORMUZ.contains(HORMUZ.lat_min, HORMUZ.lon_min - 0.001)


@pytest.mark.parametrize("region", ALL)
def test_boxes_are_sane(region):
    assert -90 <= region.lat_min < region.lat_max <= 90
    assert -180 <= region.lon_min < region.lon_max <= 180


@pytest.mark.parametrize("region", ALL)
def test_bounding_box_is_latitude_first(region):
    assert region.bounding_box() == [
        [region.lat_min, region.lon_min],
        [region.lat_max, region.lon_max],
    ]


def test_regions_do_not_overlap():
    for a, b in itertools.combinations(ALL, 2):
        separate = (
            a.lat_max < b.lat_min
            or b.lat_max < a.lat_min
            or a.lon_max < b.lon_min
            or b.lon_max < a.lon_min
        )
        assert separate, f"{a.name} overlaps {b.name}"


def test_only_control_is_flagged_as_control():
    assert [r.name for r in ALL if r.is_control] == ["control"]

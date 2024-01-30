from openeo_gfmap.temporal import TemporalContext
from openeo_gfmap.utils import quintad_intervals


def test_quintad_january():
    start_date = "2023-01-01"
    end_date = "2023-01-31"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2023-01-01", "2023-01-05"),
        ("2023-01-06", "2023-01-10"),
        ("2023-01-11", "2023-01-15"),
        ("2023-01-16", "2023-01-20"),
        ("2023-01-21", "2023-01-25"),
        ("2023-01-26", "2023-01-31"),
    ]

    assert quintad_intervals(temporal_extent) == expected

def test_quintad_april():
    start_date = "2023-04-01"
    end_date = "2023-04-30"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2023-04-01", "2023-04-05"),
        ("2023-04-06", "2023-04-10"),
        ("2023-04-11", "2023-04-15"),
        ("2023-04-16", "2023-04-20"),
        ("2023-04-21", "2023-04-25"),
        ("2023-04-26", "2023-04-30"),
    ]

    assert quintad_intervals(temporal_extent) == expected

def test_quintad_february_nonleap():
    start_date = "2023-02-01"
    end_date = "2023-02-28"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2023-02-01", "2023-02-05"),
        ("2023-02-06", "2023-02-10"),
        ("2023-02-11", "2023-02-15"),
        ("2023-02-16", "2023-02-20"),
        ("2023-02-21", "2023-02-25"),
        ("2023-02-26", "2023-02-28"),
    ]

    assert quintad_intervals(temporal_extent) == expected

def test_quitad_february_leapyear():
    start_date = "2024-02-01"
    end_date = "2024-02-29"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2024-02-01", "2024-02-05"),
        ("2024-02-06", "2024-02-10"),
        ("2024-02-11", "2024-02-15"),
        ("2024-02-16", "2024-02-20"),
        ("2024-02-21", "2024-02-25"),
        ("2024-02-26", "2024-02-29"),
    ]

    assert quintad_intervals(temporal_extent) == expected

def test_quintad_four_months():
    start_date = "2023-01-01"
    end_date = "2023-04-30"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2023-01-01", "2023-01-05"),
        ("2023-01-06", "2023-01-10"),
        ("2023-01-11", "2023-01-15"),
        ("2023-01-16", "2023-01-20"),
        ("2023-01-21", "2023-01-25"),
        ("2023-01-26", "2023-01-31"),
        ("2023-02-01", "2023-02-05"),
        ("2023-02-06", "2023-02-10"),
        ("2023-02-11", "2023-02-15"),
        ("2023-02-16", "2023-02-20"),
        ("2023-02-21", "2023-02-25"),
        ("2023-02-26", "2023-02-28"),
        ("2023-03-01", "2023-03-05"),
        ("2023-03-06", "2023-03-10"),
        ("2023-03-11", "2023-03-15"),
        ("2023-03-16", "2023-03-20"),
        ("2023-03-21", "2023-03-25"),
        ("2023-03-26", "2023-03-31"),
        ("2023-04-01", "2023-04-05"),
        ("2023-04-06", "2023-04-10"),
        ("2023-04-11", "2023-04-15"),
        ("2023-04-16", "2023-04-20"),
        ("2023-04-21", "2023-04-25"),
        ("2023-04-26", "2023-04-30"),
    ]

    assert quintad_intervals(temporal_extent) == expected

def test_quintad_july_august():
    start_date = "2023-07-01"
    end_date = "2023-08-31"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2023-07-01", "2023-07-05"),
        ("2023-07-06", "2023-07-10"),
        ("2023-07-11", "2023-07-15"),
        ("2023-07-16", "2023-07-20"),
        ("2023-07-21", "2023-07-25"),
        ("2023-07-26", "2023-07-31"),
        ("2023-08-01", "2023-08-05"),
        ("2023-08-06", "2023-08-10"),
        ("2023-08-11", "2023-08-15"),
        ("2023-08-16", "2023-08-20"),
        ("2023-08-21", "2023-08-25"),
        ("2023-08-26", "2023-08-31"),
    ]

    assert quintad_intervals(temporal_extent) == expected

def test_quintad_mid_month():
    start_date = "2023-01-02"
    end_date = "2023-01-31"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2023-01-02", "2023-01-05"),
        ("2023-01-06", "2023-01-10"),
        ("2023-01-11", "2023-01-15"),
        ("2023-01-16", "2023-01-20"),
        ("2023-01-21", "2023-01-25"),
        ("2023-01-26", "2023-01-31"),
    ]

    assert quintad_intervals(temporal_extent) == expected

def test_quintad_full_year():
    # non-leap year
    start_date = "2023-01-01"
    end_date = "2023-12-31"

    temporal_extent = TemporalContext(start_date, end_date)

    assert len(quintad_intervals(temporal_extent)) == 72

    # leap yaer
    start_date = "2024-01-01"
    end_date = "2024-12-31"

    temporal_extent = TemporalContext(start_date, end_date)

    assert len(quintad_intervals(temporal_extent)) == 72

def test_quintad_mid_month_february():
    start_date = "2024-01-31"
    end_date = "2024-03-02"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2024-01-31", "2024-01-31"),
        ("2024-02-01", "2024-02-05"),
        ("2024-02-06", "2024-02-10"),
        ("2024-02-11", "2024-02-15"),
        ("2024-02-16", "2024-02-20"),
        ("2024-02-21", "2024-02-25"),
        ("2024-02-26", "2024-02-29"),
        ("2024-03-01", "2024-03-02"),
    ]

    assert quintad_intervals(temporal_extent) == expected

def test_quintad_single_day():
    start_date = "2024-02-29"
    end_date = "2024-02-29"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2024-02-29", "2024-02-29"),
    ]

    assert quintad_intervals(temporal_extent) == expected

def test_quintad_end_month():
    start_date = "2024-02-14"
    end_date = "2024-03-01"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2024-02-14", "2024-02-15"),
        ("2024-02-16", "2024-02-20"),
        ("2024-02-21", "2024-02-25"),
        ("2024-02-26", "2024-02-29"),
        ("2024-03-01", "2024-03-01"),
    ]

    assert quintad_intervals(temporal_extent) == expected

def test_quintad_new_year():
    start_date = "2023-12-04"
    end_date = "2024-01-01"

    temporal_extent = TemporalContext(start_date, end_date)

    expected = [
        ("2023-12-04", "2023-12-05"),
        ("2023-12-06", "2023-12-10"),
        ("2023-12-11", "2023-12-15"),
        ("2023-12-16", "2023-12-20"),
        ("2023-12-21", "2023-12-25"),
        ("2023-12-26", "2023-12-31"),
        ("2024-01-01", "2024-01-01"),
    ]

    print(quintad_intervals(temporal_extent))

    assert quintad_intervals(temporal_extent) == expected
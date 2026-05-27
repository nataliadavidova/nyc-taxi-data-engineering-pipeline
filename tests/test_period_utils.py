import pytest

from period_utils import generate_month_periods, validate_month_period_range


def test_generate_month_periods_for_single_month():
    assert generate_month_periods("2024", "05", "2024", "05") == [
        ("2024", "05")
    ]


def test_generate_month_periods_for_same_year_range():
    assert generate_month_periods("2024", "01", "2024", "03") == [
        ("2024", "01"),
        ("2024", "02"),
        ("2024", "03"),
    ]


def test_generate_month_periods_across_years():
    assert generate_month_periods("2024", "11", "2025", "02") == [
        ("2024", "11"),
        ("2024", "12"),
        ("2025", "01"),
        ("2025", "02"),
    ]


def test_generate_month_periods_accepts_int_values():
    assert generate_month_periods(2024, 1, 2024, 2) == [
        ("2024", "01"),
        ("2024", "02"),
    ]


@pytest.mark.parametrize(
    "start_year,start_month,end_year,end_month",
    [
        ("2024", "00", "2024", "01"),
        ("2024", "13", "2024", "01"),
        ("2024", "01", "2024", "00"),
        ("2024", "01", "2024", "13"),
    ],
)
def test_validate_month_period_range_rejects_invalid_months(
    start_year,
    start_month,
    end_year,
    end_month,
):
    with pytest.raises(ValueError, match="must be between 1 and 12"):
        validate_month_period_range(
            start_year,
            start_month,
            end_year,
            end_month,
        )


def test_validate_month_period_range_rejects_start_after_end():
    with pytest.raises(ValueError, match="start period must be earlier"):
        validate_month_period_range("2024", "06", "2024", "05")


def test_validate_month_period_range_returns_normalized_int_values():
    assert validate_month_period_range("2024", "01", "2024", "12") == (
        2024,
        1,
        2024,
        12,
    )

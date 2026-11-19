import pytest

from src.common.quality import build_check, fail_if_critical


def test_check_with_failed_rows_does_not_pass():
    check = build_check("flights", "sin_zona_horaria", 3, 100, True)
    assert check["passed"] is False
    assert check["failed_rows"] == 3
    assert check["total_rows"] == 100


def test_check_without_failed_rows_passes():
    check = build_check("flights", "sin_zona_horaria", 0, 100, True)
    assert check["passed"] is True


def test_critical_check_that_fails_stops_the_task():
    checks = [build_check("flights", "id_repetido", 2, 100, True)]
    with pytest.raises(RuntimeError):
        fail_if_critical(checks)


def test_non_critical_check_that_fails_does_not_stop_the_task():
    checks = [build_check("weather_hourly", "horas_repetidas", 5, 100, False)]
    fail_if_critical(checks)   # no tiene que lanzar error

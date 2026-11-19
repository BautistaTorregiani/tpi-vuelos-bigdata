from src.common.quality import build_check


def test_check_with_failed_rows_does_not_pass(spark):
    row = build_check(spark, "flights", "sin_zona_horaria", 3, True, "b1").first()
    assert row["failed_rows"] == 3
    assert row["passed"] is False
    assert row["is_critical"] is True
    assert row["checked_at"] is not None


def test_check_without_failed_rows_passes(spark):
    row = build_check(spark, "flights", "sin_zona_horaria", 0, True, "b1").first()
    assert row["passed"] is True

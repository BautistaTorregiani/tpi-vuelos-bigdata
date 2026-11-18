from src.silver.airlines import clean_airlines


def test_airlines_columns_are_renamed(spark):
    df = spark.createDataFrame([("AS", "Alaska Airlines Inc.", "manual")],
                               "IATA_CODE string, AIRLINE string, batch_id string")
    row = clean_airlines(df).first()
    assert row["iata_code"] == "AS"
    assert row["airline_name"] == "Alaska Airlines Inc."
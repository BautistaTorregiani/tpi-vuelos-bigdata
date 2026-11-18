from src.bronze.csv_ingestion import read_raw_csv, add_ingestion_metadata


def test_read_raw_csv_keeps_strings(spark, tmp_path):
    file = tmp_path / "airlines.csv"
    file.write_text("IATA_CODE,AIRLINE\nAA,American Airlines\nDL,Delta\n")

    df = read_raw_csv(spark, str(file))

    assert df.count() == 2
    assert all(dtype == "string" for _, dtype in df.dtypes)


def test_add_ingestion_metadata(spark, tmp_path):
    file = tmp_path / "airlines.csv"
    file.write_text("IATA_CODE,AIRLINE\nAA,American Airlines\n")

    df = add_ingestion_metadata(read_raw_csv(spark, str(file)), batch_id="batch-123")
    row = df.collect()[0]

    assert row["batch_id"] == "batch-123"
    assert row["source_file"].endswith("airlines.csv")
    assert row["ingestion_ts"] is not None
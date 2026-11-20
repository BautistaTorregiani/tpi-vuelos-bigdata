from src.common.catalog_comments import build_comment_statements


def test_comment_statements(spark):
    statements = build_comment_statements("vuelos_dev.gold.dim_airline", "Aerolíneas",
                                          [("airline_key", "Código de la aerolínea")])
    assert statements[0] == "COMMENT ON TABLE vuelos_dev.gold.dim_airline IS 'Aerolíneas'"
    assert statements[1] == ("ALTER TABLE vuelos_dev.gold.dim_airline ALTER COLUMN `airline_key` "
                             "COMMENT 'Código de la aerolínea'")


def test_quotes_are_escaped(spark):
    statements = build_comment_statements("t", "Vuelos de Hawai'i", [])
    assert statements[0] == "COMMENT ON TABLE t IS 'Vuelos de Hawai''i'"
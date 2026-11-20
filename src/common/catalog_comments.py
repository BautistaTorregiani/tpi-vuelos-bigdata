def escape_quotes(text):
    """En SQL, una comilla simple dentro de un texto se escribe duplicada."""
    return text.replace("'", "''")


def build_comment_statements(table, table_comment, column_comments):
    """Arma las sentencias SQL que ponen el comentario de la tabla y de cada columna."""
    statements = [f"COMMENT ON TABLE {table} IS '{escape_quotes(table_comment)}'"]
    for column, description in column_comments:
        statements.append(f"ALTER TABLE {table} ALTER COLUMN `{column}` COMMENT '{escape_quotes(description)}'")
    return statements


def apply_comments(spark, table, table_comment, column_comments):
    """Aplica en Unity Catalog los comentarios de una tabla y de sus columnas."""
    for statement in build_comment_statements(table, table_comment, column_comments):
        spark.sql(statement)
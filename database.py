from contextlib import contextmanager


# --- Create Tables ---
CREATE_COMPANIES = """
    CREATE TABLE IF NOT EXISTS companies (
        id SERIAL PRIMARY KEY,
        name TEXT,
        ticker TEXT
    );
"""
CREATE_BARS = """
    CREATE TABLE IF NOT EXISTS bars (
        id SERIAL PRIMARY KEY,
        ticker TEXT,
        timestamp REAL,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        adj_close NUMERIC,
        volume BIGINT
    );
"""


@contextmanager
def get_cursor(connection):
    with connection:
        with connection.cursor() as cursor:
            yield cursor


def create_tables(connection):
    with get_cursor(connection) as cursor:
        cursor.execute(CREATE_COMPANIES)
        cursor.execute(CREATE_BARS)


if __name__ == '__main__':
    from connection_pool import get_connection
    with get_connection() as connection:
        create_tables(connection)

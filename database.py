# import os
from io import StringIO
# import pandas as pd

from contextlib import contextmanager
from psycopg2.errorcodes import UNIQUE_VIOLATION
from psycopg2 import errors


# --- Reset Database ---
DROP_TABLE_COMPANIES = "DROP TABLE IF EXISTS companies;"
DROP_TABLE_BARS = "DROP TABLE IF EXISTS bars;"

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
        date DATE,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        adj_close NUMERIC,
        volume BIGINT,
        UNIQUE (ticker, date)
    );
"""

SELECT_LAST_TICKER = """
    SELECT MAX(date) AS last_row FROM bars
    WHERE ticker = %s;
"""
SELECT_FIRST_TICKER = """
    SELECT MIN(date) AS last_row FROM bars
    WHERE ticker = %s;
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


def drop_tables(connection):
    with get_cursor(connection) as cursor:
        cursor.execute(DROP_TABLE_COMPANIES)
        cursor.execute(DROP_TABLE_BARS)


def get_last_ts(connection, ticker):
    with get_cursor(connection) as cursor:
        cursor.execute(SELECT_LAST_TICKER, (ticker,))
        return cursor.fetchone()[0]


def get_first_ts(connection, ticker):
    with get_cursor(connection) as cursor:
        cursor.execute(SELECT_FIRST_TICKER, (ticker,))
        return cursor.fetchone()[0]


def bulk_insert_bars(connection, df):
    # Initialize a string buffer
    sio = StringIO()
    # Write the Pandas DataFrame as a csv to the buffer
    sio.write(df.to_csv(index=None, header=None))
    # Be sure to reset the position to the start of the stream
    sio.seek(0)

    with get_cursor(connection) as cursor:
        try:
            cursor.copy_from(sio, 'bars', columns=df.columns, sep=',')
            connection.commit()
        except errors.lookup(UNIQUE_VIOLATION):
            print('Error: Rows already exists!')


if __name__ == '__main__':
    from connection_pool import get_connection
    with get_connection() as connection:
        drop_tables(connection)
        create_tables(connection)
        # get_last_ts(connection, 'NHY.OL')

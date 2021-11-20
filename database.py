from io import StringIO
from contextlib import contextmanager
from psycopg2.errorcodes import UNIQUE_VIOLATION
from psycopg2 import errors


# --- Reset Database ---
DROP_TABLE_COMPANIES = "DROP TABLE IF EXISTS companies;"
DROP_TABLE_BARS = "DROP TABLE IF EXISTS bars;"
DROP_TABLE_NOT_FOUND = "DROP TABLE IF EXISTS not_found;"

# --- Create Tables ---
CREATE_COMPANIES = """
    CREATE TABLE IF NOT EXISTS companies (
        ticker TEXT PRIMARY KEY,
        name TEXT,
        exchange TEXT,
        sector TEXT
    );
"""
CREATE_COMPANIES_ = """
    CREATE TABLE IF NOT EXISTS companies (
        id SERIAL PRIMARY KEY,
        ticker TEXT,
        name TEXT,
        city TEXT,
        country TEXT,
        currency TEXT,
        exchange TEXT,
        sector TEXT,
        industry TEXT,
        UNIQUE (ticker)
    );
"""
CREATE_EXCHANGES = """
    CREATE TABLE IF NOT EXISTS exchanges (
        id SERIAL PRIMARY KEY,
        name TEXT,
        UNIQUE (name)
    );
"""
CREATE_SECTORS = """
    CREATE TABLE IF NOT EXISTS sectors (
        id SERIAL PRIMARY KEY,
        name TEXT,
        UNIQUE (name)
    );
"""
CREATE_INDUSTRIES = """
    CREATE TABLE IF NOT EXISTS industries (
        id SERIAL PRIMARY KEY,
        name TEXT,
        UNIQUE (name)
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
CREATE_NOT_FOUND = """
    CREATE TABLE IF NOT EXISTS not_found (
        id SERIAL PRIMARY KEY,
        ticker TEXT,
        UNIQUE (ticker)
    );
"""

# --- SELECT FROM DATABASE ---
SELECT_LAST_TICKER = """
    SELECT MAX(date) AS last_row FROM bars
    WHERE ticker = %s;
"""
SELECT_FIRST_TICKER = """
    SELECT MIN(date) AS first_row FROM bars
    WHERE ticker = %s;
"""

SELECT_COMPANIES_WHERE_EXCHANGE = """
    SELECT ticker FROM companies
    WHERE exchange = %s
    LIMIT %s;
"""

SELECT_COMPANIES_WHERE_TICKER = """
    SELECT ticker FROM companies
    WHERE ticker = %s;
"""

SELECT_TICKERS_NOT_FOUND = """
    SELECT ticker from not_found;
"""

# --- INSERT INTO DATABASE ---
INSERT_COMPANIE_RETURN_TICKER = """
    INSERT INTO companies
    (ticker, name, exchange, sector)
    VALUES (%s, %s, %s, %s)
    RETURNING ticker;
"""
INSERT_COMPANIE_RETURN_ID = """
    INSERT INTO companies
    (ticker, name, city, country, currency, exchange, sector, industry)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
"""
INSERT_TICKER_INTO_NOT_FOUND = """
    INSERT INTO not_found
    (ticker)
    VALUES (%s);
"""

# SELECT b.*, c.exchange, c.sector
# FROM bars b, companies c
# WHERE c.exchange = 'Oslo'
# AND c.sector LIKE 'Health Care%'
# AND b.ticker = c.ticker
# AND b.date >= '2021-11-01'
# ORDER BY b.date DESC


@contextmanager
def get_cursor(connection):
    with connection:
        with connection.cursor() as cursor:
            yield cursor


def create_tables(connection):
    with get_cursor(connection) as cursor:
        # cursor.execute(CREATE_COMPANIES_)
        # cursor.execute(CREATE_BARS)
        cursor.execute(CREATE_NOT_FOUND)


def drop_tables(connection):
    with get_cursor(connection) as cursor:
        # cursor.execute(DROP_TABLE_COMPANIES)
        # cursor.execute(DROP_TABLE_BARS)
        cursor.execute(DROP_TABLE_NOT_FOUND)


def get_last_ts(connection, ticker):
    with get_cursor(connection) as cursor:
        cursor.execute(SELECT_LAST_TICKER, (ticker,))
        return cursor.fetchone()[0]


def get_first_ts(connection, ticker):
    with get_cursor(connection) as cursor:
        cursor.execute(SELECT_FIRST_TICKER, (ticker,))
        return cursor.fetchone()[0]


def get_not_found_tickers(connection):
    with get_cursor(connection) as cursor:
        cursor.execute(SELECT_TICKERS_NOT_FOUND)
        return cursor.fetchall()


def get_tickers(connection, exchange, limit):
    with get_cursor(connection) as cursor:
        cursor.execute(SELECT_COMPANIES_WHERE_EXCHANGE, (exchange, limit))
        return cursor.fetchall()


def companies_ticker_exist(connection, ticker):
    with get_cursor(connection) as cursor:
        cursor.execute(SELECT_COMPANIES_WHERE_TICKER, (ticker,))
        if cursor.fetchone():
            return True
    return False


def add_company(
    connection, ticker, name, city,
    country, currency, exchange, sector,
    industry
):
    with get_cursor(connection) as cursor:
        try:
            cursor.execute(
                INSERT_COMPANIE_RETURN_ID,
                (
                    ticker, name, city, country,
                    currency, exchange, sector,
                    industry
                )
            )
        except errors.lookup(UNIQUE_VIOLATION):
            return None

        return cursor.fetchone()[0]


def add_ticker_not_found(connection, ticker):
    with get_cursor(connection) as cursor:
        try:
            cursor.execute(
                INSERT_TICKER_INTO_NOT_FOUND,
                (ticker,)
            )
        except errors.lookup(UNIQUE_VIOLATION):
            print(f"Ticker: {ticker} exists")


def bulk_insert_bars(connection, df, table: str):
    # Initialize a string buffer
    sio = StringIO()
    # Write the Pandas DataFrame as a csv to the buffer
    sio.write(df.to_csv(index=None, header=None))
    # Be sure to reset the position to the start of the stream
    sio.seek(0)

    with get_cursor(connection) as cursor:
        try:
            cursor.copy_from(sio, table, columns=df.columns, sep=',')
            connection.commit()
            return True
        except errors.lookup(UNIQUE_VIOLATION):
            return 'Error: Rows already exists!'


if __name__ == '__main__':
    from connection_pool import get_connection
    with get_connection() as connection:
        drop_tables(connection)
        create_tables(connection)
        # get_last_ts(connection, 'NHY.OL')

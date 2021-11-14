import yfinance as yf
import pandas as pd

import database
from connection_pool import get_connection


def rename_columns(df):
    new_columns = {
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Adj Close': 'adj_close',
        'Volume': 'volume'
    }
    df.rename(columns=new_columns, inplace=True)
    return df


def download_history(ticker, period='1y'):
    df = yf.download(
        ticker.strip(),
        period=period,
        progress=False,
        show_errors=False,
        auto_adjust=False,
    )
    df.reset_index(inplace=True)
    df = rename_columns(df)
    df['date'] = pd.to_datetime(df['date']).apply(lambda x: x.date())
    df['ticker'] = ticker
    return df


def filter_data_by_ts(df, ticker):
    with get_connection() as connection:
        last_ts = database.get_last_ts(connection, ticker)
        first_ts = database.get_first_ts(connection, ticker)
    if last_ts is None:
        return df
    mask = (df['date'] > last_ts) | (df['date'] < first_ts)
    return df[mask]


def prepare_companies_file_for_db(
    ticker_file, ticker_column='ticker',
    name_column='Company', sector_column='sector',
    exchange='Oslo'
):
    df_raw = pd.read_csv(ticker_file)
    df_db = pd.DataFrame(columns=['ticker', 'name', 'exchange', 'sector'])
    df_db['ticker'] = df_raw[ticker_column]
    df_db['name'] = df_raw[name_column].str.replace(',', '')
    df_db['exchange'] = exchange
    df_db['sector'] = df_raw[sector_column].str.replace(',', '')
    # Remove dublicates tickers, is a primary key
    df_db = df_db.drop_duplicates(subset=['ticker'])
    return df_db


if __name__ == '__main__':
    # Oslo.csv
    df = prepare_companies_file_for_db(
        ticker_file='data_files/companies/oslo.csv',
        ticker_column='Ticker',
        name_column='Company',
        sector_column='GICS Industry[5]',
        exchange='Oslo'
    )

    # # SP500.csv
    # df = prepare_companies_file_for_db(
    #     ticker_file='data_files/companies/sp500.csv',
    #     ticker_column='Ticker',
    #     name_column='Security',
    #     sector_column='GICS Sector',
    #     exchange='Sp500'
    # )

    # # stockholm.csv
    # df = prepare_companies_file_for_db(
    #     ticker_file='data_files/companies/stockholm.csv',
    #     ticker_column='Ticker',
    #     name_column='Company Name',
    #     sector_column='Sector',
    #     exchange='Stockholm'
    # )

    # df = download_history('NHY.OL')
    # df = filter_data_by_ts(df, ticker='NHY.OL')

    if len(df) > 0:
        with get_connection() as connection:
            ret = database.bulk_insert_bars(connection, df, 'companies')
            if ret is True:
                print(f"Inserted {len(df)} rows!")
            else:
                print(ret)
    else:
        print('Nothing to insert!')

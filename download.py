import yfinance as yf
import database
from connection_pool import get_connection


def rename_columns(df):
    new_columns = {
        'Date': 'ts',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Adj Close': 'adj_close',
        'Volume': 'volume'
    }
    df.rename(columns=new_columns, inplace=True)
    return df


def download_history(ticker, period='5y'):
    df = yf.download(
        ticker.strip(),
        period=period,
        progress=False,
        show_errors=False,
        auto_adjust=False,
    )
    df.reset_index(inplace=True)
    df = rename_columns(df)
    df['ts'] = df['ts'].values.tolist()
    df['ticker'] = ticker
    return df


if __name__ == '__main__':
    df = download_history('NHY.OL')
    with get_connection() as connection:
        database.bulk_insert_bars(connection, df)

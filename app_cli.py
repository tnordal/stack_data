import download
from connection_pool import get_connection
import database
import yfinance as yf
import pickle
import pandas as pd
pd.options.mode.chained_assignment = None

PATH_COMPANIES_FILES = 'data_files/companies/'

MAIN_MENU_PROMPT = """
--- Main Menu ---

1) Update price data (bars)
2) Update single ticker price data
3) Update Companies from csv file
4) Add a Company
q) Exit Main Menu

Enter your choise:"""

# --- Menu Prompts ---


def update_bars_promt():
    exchange = input('Enter Exchange (Oslo, Sp500, Stockholm):')
    period = input('Enter period (1y, 2y...):')
    max_tickers = input('Enter max tickers to update:')

    max_tickers = 999 if max_tickers == '' else int(max_tickers)
    update_bars(exchange, period, max_tickers)


def update_ticker_prompt():
    ticker = input('Enter ticker:')
    period = input('Enter period (1y, 2y...):')
    update_ticker(ticker=ticker, period=period)


def update_companies_promt():
    print('Update companies from csv')
    csv_file = input('Enter name csv-file:')

    col_ticker = input('Enter column name for the ticker [Ticker]:')
    if not col_ticker:
        col_ticker = 'Ticker'

    max_tickers = input('Enter max tickers to update [9999]:')
    max_tickers = int(max_tickers) if max_tickers else 9999

    print(
        f"update_companies({csv_file}, {col_ticker}, {max_tickers})"
    )
    update_companies(
        ticker_file=csv_file,
        ticker_column=col_ticker,
        max_tickers=max_tickers
    )


def add_company_promt():
    print('Add companie')
    print('Not working just now')
    # ticker = input('Enter ticker:').upper()
    # companie = input('Enter Companie name:')
    # exchange = input('Enter Exchange name:')
    # sector = input('Enter a Sector:')
    # print(f"Add {companie} in sector {sector} with {ticker} as ticker")
    # add_companie(ticker, companie, exchange, sector)

# --- Menu Functions ---


def update_ticker(ticker, period):
    print(f"Update {ticker} for period of {period}")
    df = download.download_history(ticker, period)
    df = download.filter_data_by_ts(df, ticker)
    # TODO Force volume column to int
    df.dropna(axis=0, inplace=True)
    df = df.astype({'volume': 'int32'}, copy=True)
    bulk_insert(df, 'bars')


def update_bars(exchange, period, max_tickers):
    print(f"Update {exchange} for period of {period}")
    # Get list of tickers
    with get_connection() as connection:
        tickers = database.get_tickers(connection, exchange, max_tickers)

    # loop ticker list
    for ticker in tickers:
        # bulk insert for each ticker
        update_ticker(ticker[0], period)


# --- Helper Functions ---
def list_to_file(list, filename):
    with open(filename, 'wb') as f:
        pickle.dump(list, f)


def get_not_found_list(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)


def not_found_from_db():
    with get_connection() as connection:
        tickers = database.get_not_found_tickers(connection)
    return [ticker[0] for ticker in tickers]


def update_companies(ticker_file, ticker_column, max_tickers):
    not_found = not_found_from_db()
    not_found_new = []
    try:
        ticker_file = PATH_COMPANIES_FILES + ticker_file
        df = pd.read_csv(ticker_file)
        tickers = df[ticker_column].to_list()

    except FileNotFoundError:
        print('File not found:', ticker_file)
        exit(1)

    counter = 0
    tickers_added = 0
    for ticker in tickers:
        if ticker not in not_found:
            with get_connection() as connection:
                if not database.companies_ticker_exist(connection, ticker):
                    ticker_info = yf.Ticker(ticker).info
                    try:
                        database.add_company(
                            connection=connection,
                            ticker=ticker_info['symbol'],
                            name=ticker_info['shortName'],
                            city=ticker_info['city'],
                            country=ticker_info['country'],
                            currency=ticker_info['currency'],
                            exchange=ticker_info['exchange'],
                            sector=ticker_info['sector'],
                            industry=ticker_info['industry']
                        )
                        print(ticker, 'added to Database')
                        tickers_added += 1
                        counter += 1
                    except KeyError:
                        not_found.append(ticker)
                        not_found_new.append(ticker)
                        with get_connection() as connection:
                            database.add_ticker_not_found(connection, ticker)
                        print(f"Ticker {ticker} not found!")
                    if counter > max_tickers:
                        break
    if not_found_new:
        print('List of tickers not found:', not_found_new)

    print('Tickers added: ', tickers_added)


def add_company(ticker, name, exchange, sector, industry):
    with get_connection() as connection:
        new_id = database.add_company(
            connection=connection,
            ticker=ticker,
            name=name,
            exchange=exchange,
            sector=sector,
            industry=industry
        )
    if new_id:
        print(f"Ticker {ticker} added")
    else:
        print(f"Ticker {ticker} already exists in DB!")


def bulk_insert(df, table):
    if len(df) > 0:
        with get_connection() as connection:
            ret = database.bulk_insert_bars(connection, df, table)
            if ret is True:
                print(f"Inserted {len(df)} rows!")
            else:
                print(ret)
    else:
        print('Nothing to insert!')


MAIN_MENU_OPTIONS = {
    '1': update_bars_promt,
    '2': update_ticker_prompt,
    '3': update_companies_promt,
    '4': add_company_promt
}


def main_menu():
    while (selection := input(MAIN_MENU_PROMPT)) != 'q':
        try:
            MAIN_MENU_OPTIONS[selection]()
        except KeyError:
            print('Invalid Input Selection. Please trye again!')


if __name__ == '__main__':
    main_menu()

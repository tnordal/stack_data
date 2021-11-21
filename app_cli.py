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
    # exchange = input('Enter Exchange (Oslo, Sp500, Stockholm):')
    exchange = get_exchange_prompt()
    if exchange:
        period = input('Enter period (1y, 2y...):')
        max_tickers = input('Enter max tickers to update:')
        max_tickers = 999 if max_tickers == '' else int(max_tickers)
        update_bars(exchange, period, max_tickers)


def get_exchange_prompt():
    with get_connection() as connection:
        exchanges = database.get_exchanges(connection)
    menu = ''
    menu_dict = {}
    for i, exchange in enumerate(exchanges):
        item = f"{i}) {exchange[0]} \n"
        menu += item
        menu_dict[i] = exchange[0]
    menu += "Enter the number of one of theese exchanges:"
    select = input(menu)
    try:
        select = int(select)
        print(f"You select {select} exchange :{menu_dict[select]}")
        return menu_dict[select]
    except ValueError:
        print('Not a number')
        return None
    except KeyError:
        print('Not a valied number')
        return None


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
    print('Add company')
    ticker = input('Enter ticker:').upper()
    if ticker:
        add_company(ticker)
    else:
        print('Error: Enter a valid ticker!')


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
                if not database.company_ticker_exist(connection, ticker):
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
                    if counter >= max_tickers:
                        break
    if not_found_new:
        print('List of tickers not found:', not_found_new)

    print('Tickers added: ', tickers_added)


def add_company(ticker):
    # Check if ticker in DB
    with get_connection() as connection:
        ticker_exists = database.company_ticker_exist(connection, ticker)
        if not ticker_exists:
            # Download ticker if not in DB
            ticker_info = yf.Ticker(ticker).info
            if 'symbol' in ticker_info:
                # Add ticker info to DB
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
                # Remove ticker from not_found table
                database.delete_ticker_not_found(connection, ticker)
            else:
                print(f"{ticker} Not exist in Yahoo")
        else:
            print(f"Ticker {ticker} already in Database")


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

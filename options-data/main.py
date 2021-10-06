from tda import auth
import pandas as pd
import datetime
from datetime import date, time, timezone
import time
import pandas_market_calendars as mcal
from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import QueuePool
import os
import config
import traceback


def dt_now():
    """Get current datetime"""
    dt_now = datetime.datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return dt_now


def opencheck():
    """Check if markets are currently open using pandas_market_calendars"""
    trading_day = mcal.get_calendar('NYSE').schedule(start_date=date.today(), end_date=date.today())
    try:
        open_time = trading_day.iloc[0][0]
        close_time = trading_day.iloc[0][1]
        if open_time < datetime.datetime.now(tz=timezone.utc) < close_time:
            market_open = True
        else:
            market_open = False
    except Exception:
        market_open = False
    return market_open


def logins():
    """Login and Connect to Database"""
    # Login to TD-Ameritrade
    token_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'ameritrade-credentials.json')
    api_key = config.api_key
    redirect_uri = config.redirect_uri
    try:
        c = auth.client_from_token_file(token_path, api_key)
    except FileNotFoundError:
        from selenium import webdriver
        with webdriver.Chrome() as driver:
            c = auth.client_from_login_flow(
                driver, api_key, redirect_uri, token_path)

    # Connect to Database
    engine = create_engine(config.psql, poolclass=QueuePool, pool_size=1, max_overflow=20, pool_recycle=3600,
                           pool_pre_ping=True, isolation_level='AUTOCOMMIT')
    return c, engine


def getsymbols():
    """Get a list of symbols to use"""
    """Example 1: Read from Database"""
    # engine = create_engine(config.psql)
    # symbol_df = pd.read_sql_query('select ticker, market_cap from companies where market_cap >= 900000000', con=engine)
    # symbol_df = symbol_df.sort_values("market_cap", ascending=False)
    # symbols = symbol_df['ticker'].tolist()

    """Example 2: Get S&P500 symbols from wikipedia"""
    symbol_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    symbol_df = pd.DataFrame(symbol_df[0:][0])
    symbols = symbol_df.Symbol.to_list()

    """Example 3: Create a list of symbols"""
    # symbols = ['UVXY', 'DUST', 'VZ']

    # Convert symbols to TDA options format
    symbols = [x.replace(' ', '/') for x in symbols]
    symbols = [x.replace('.', '/') for x in symbols]
    return symbols


def table_mgmt(engine):
    print(dt_now(), 'Checking for database...')
    table_exists = inspect(engine).dialect.has_table(engine.connect(), config.options_sql_table_name)
    if not table_exists:
        print(dt_now(), 'Table does not exist in database, creating a new table...')
        create_table_statement = f"""CREATE TABLE IF NOT EXISTS {config.options_sql_table_name}("putCall" character varying,
        symbol character varying, description character varying, "exchangeName" text, bid numeric, 
        ask numeric, last numeric, mark numeric, "bidSize" bigint, "askSize" bigint, 
        "bidAskSize" character varying, "lastSize" bigint, "highPrice" numeric, "lowPrice" numeric, 
        "openPrice" numeric, "closePrice" numeric, "totalVolume" bigint, "tradeDate" text, 
        "tradeTimeInLong" bigint, "quoteTimeInLong" bigint, "netChange" numeric, volatility numeric, 
        delta numeric, gamma numeric, theta numeric, vega numeric, rho numeric, "openInterest" bigint, 
        "timeValue" numeric, "theoreticalOptionValue" numeric, "theoreticalVolatility" numeric, 
        "optionDeliverablesList" text, "strikePrice" numeric, "expirationDate" bigint, "daysToExpiration" bigint, 
        "expirationType" text, "lastTradingDay" bigint, multiplier bigint, "settlementType" text, "deliverableNote" text, 
        "isIndexOption" text, "percentChange" numeric, "markChange" numeric, "markPercentChange" numeric, 
        "nonStandard" boolean, "inTheMoney" boolean, mini boolean, uticker character varying, 
        tdate timestamp with time zone, "intrinsicValue" numeric, "pennyPilot" boolean, 
        save_date timestamp with time zone);"""
        try:
            with engine.connect() as conn:
                conn.execute(create_table_statement)
            print(dt_now(), 'Table created successfully')
        except Exception:
            print(Exception)
    else:
        print(dt_now(), 'Table exists in database, continuing...')
    time.sleep(0.25)
    return None


def unix_convert(ts):
    ts = int(ts/1000)
    tdate = datetime.datetime.utcfromtimestamp(ts)
    return tdate


# Core options data code
def options_chain(symbol, c):
    while True:
        try:
            print(dt_now(), "Retrieving:", symbol)
            # Create empty list
            options_dict = []
            # API call to https://developer.tdameritrade.com/option-chains/apis/get/marketdata/chains
            r = c.get_option_chain(symbol)
            # Sleep if rate-limit is exceeded
            if r.status_code == 429:
                time.sleep(5)
            else:
                query = r.json()
                # Start rate-limit timer
                timer = time.time()
                # Flatten nested JSON
                for contr_type in ['callExpDateMap', 'putExpDateMap']:
                    contract = dict(query)[contr_type]
                    expirations = contract.keys()
                    for expiry in list(expirations):
                        strikes = contract[expiry].keys()
                        for st in list(strikes):
                            entry = contract[expiry][st][0]
                            # Create list of dictionaries with the flattened JSON
                            options_dict.append(entry)
                # Check if list is empty
                if options_dict:
                    # Remove any unknown keys from response (in case TDA changes the API response without warning)
                    key_count = len(options_dict[0].keys())
                    if key_count > 49:
                        # List of known keys
                        keys = ["putCall", "symbol", "description", "exchangeName", "bid", "ask", "last", "mark", "bidSize"
                                "askSize", "bidAskSize", "lastSize", "highPrice", "lowPrice", "openPrice", "closePrice",
                                "totalVolume", "tradeDate", "tradeTimeInLong", "quoteTimeInLong", "netChange", "volatility", "delta",
                                "gamma", "theta", "vega", "rho", "openInterest", "timeValue", "theoreticalOptionValue",
                                "theoreticalVolatility", "optionDeliverablesList", "strikePrice", "expirationDate",
                                "daysToExpiration", "expirationType", "lastTradingDay", "multiplier", "settlementType",
                                "deliverableNote", "isIndexOption", "percentChange", "markChange", "markPercentChange",
                                "nonStandard", "inTheMoney", "mini", "intrinsicValue", "pennyPilot"]
                        # Drop unknown keys
                        options_dict = [{k: single[k] for k in keys if k in single} for single in options_dict]
                        # Warn user of new keys
                        print(dt_now(), 'New fields detected! Check API documentation: '
                              'https://developer.tdameritrade.com/option-chains/apis/get/marketdata/chains')

                    # Check if there are less than the expected number of fields
                    key_count = len(options_dict[0].keys())  # Update after dropping unknown keys
                    if key_count < 49:
                        print(dt_now(), f'Warning, potential data errors for: {symbol}: '
                              'https://downdetector.com/status/td-ameritrade/')
                else:
                    print(dt_now(), f'No data available for: {symbol}')

                # Convert dictionary to dataframe
                df = pd.DataFrame(options_dict)

                # Wait for rate-limiting
                # todo: add exponential backoff
                z = time.time()-timer
                if z > 0.51:
                    time.sleep(0.05)
                else:
                    sleep = 0.51 - z
                    time.sleep(sleep)
        except Exception:
            traceback.print_exc()
            time.sleep(1)
            continue
        break
    return df


def get_data(symbols, c, engine):
    """Get options data and save to a database"""
    for symbol in symbols:
        # Create empty dataframe
        df = pd.DataFrame()
        # Get data from API
        result = options_chain(symbol, c)
        try:
            df = pd.concat([df, result])
            if not df.empty:
                # Add underlying symbol to dataframe
                df["uticker"] = symbol
                # Convert exact time to readable format
                df['tdate'] = df['quoteTimeInLong'].map(lambda x: unix_convert(x))
                # Remove JSON
                df['optionDeliverablesList'] = df['optionDeliverablesList'].map(lambda x: str(x))
                # Add time of retrieval to dataframe
                df["save_date"] = datetime.datetime.now(tz=timezone.utc).replace(hour=20, minute=0, second=0, microsecond=0) \
                              if datetime.datetime.now(tz=timezone.utc).time() > datetime.time(20, 0, 0) \
                              else datetime.datetime.now(tz=timezone.utc)
                
                # Save to Database
                # todo: optimize save method (remove pandas)
                df.to_sql(config.options_sql_table_name, engine, if_exists='append', index=False, method='multi')

                # Save to CSV (coming soon....)
                # df.to_csv('optionsdata.csv', index=False, chunksize=10000)
                # print(df)
            del df
        except Exception:
            traceback.print_exc()
    return None


def main():
    """Main"""
    print(dt_now(), "Welcome, good luck and have fun!")

    """Login/Connections"""
    c, engine = logins()
    table_mgmt(engine)  # Check for options data table, create table if one doesn't exist

    """Begin Main Loop"""
    while True:
        symbols = getsymbols()
        market_open = opencheck()  # Check if markets are open
        while market_open:
            market_open = opencheck()  # Check if markets are still open
            print(dt_now(), "Markets are open!")
            while market_open:
                market_open = opencheck()  # Check if markets are still open
                print(dt_now(), f'Symbols list: {symbols}')
                print(dt_now(), 'Number of symbols:', len(symbols))
                if market_open:
                    try:
                        get_data(symbols, c, engine)
                    except Exception:
                        traceback.print_exc()
                else:
                    print(dt_now(), "Markets are closed, have a nice day!")
                    break
            else:
                print(dt_now(), "Markets are closed, have a nice day!")
                break
        else:
            time.sleep(1)  # Sleep until the markets are open again
        continue
    return None


if __name__ == "__main__":
    main()

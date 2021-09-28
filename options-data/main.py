"""--------------------------------------------------------------------------------------------------------------------
Copyright 2021 Market Maker Lite, LLC (MML)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is
distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

This file is part of the MML Open Source Library (www.github.com/MarketMakerLite)
--------------------------------------------------------------------------------------------------------------------"""
from tda import auth
import pandas as pd
from datetime import datetime, date, time, timezone, timedelta
import time
import pandas_market_calendars as mcal
from sqlalchemy import create_engine
import os
import config
import traceback


# Check if markets are currently open using pandas_market_calendars
def OpenCheck():
    now = datetime.now(tz=timezone.utc)
    trading_day = mcal.get_calendar('NYSE').schedule(start_date=date.today(), end_date=date.today())
    try:
        open_time = trading_day.iloc[0][0]
        close_time = trading_day.iloc[0][1]
        if close_time > now > open_time:
            market_open = True
        else:
            market_open = False
    except Exception:
        market_open = False
    return market_open


# Login to TDA or create token if one doesn't exist
def loginTDA():
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
    return c


# Get a list of symbols to use
def getsymbols():
    """Example 1: Read from Database"""
    # engine = create_engine(config.psql)
    # symbol_df = pd.read_sql_query('select * from companies where market_cap >= 900000000', con=engine)
    # symbol_df = symbol_df.sort_values("market_cap", ascending=False)
    # symbols = symbol_df['ticker'].tolist()

    """Example 2: Get S&P500 symbols from wikipedia"""
    symbol_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    symbol_df = pd.DataFrame(symbol_df[0:][0])
    symbols = symbol_df.Symbol.to_list()

    # Convert symbols to TDA options format
    symbols = [x.replace(' ', '/') for x in symbols]
    symbols = [x.replace('.', '/') for x in symbols]
    return symbols


# Core options data code
def options_chain(symbol, c):
    while True:
        try:
            print(datetime.today(), datetime.now(tz=timezone.utc).strftime("%H:%M:%S"), "Parsing... ", symbol)
            options_dict = []
            o = c.get_option_chain(symbol)
            query = o.json()
            for contr_type in ['callExpDateMap', 'putExpDateMap']:
                contract = dict(query)[contr_type]
                expirations = contract.keys()
                for expiry in list(expirations):
                    strikes = contract[expiry].keys()
                    for st in list(strikes):
                        entry = contract[expiry][st][0]
                        options_dict.append(entry)

            # Convert dictionary to dataframe
            df = pd.DataFrame(options_dict)

            # Add underlying symbol to dataframe
            df["uticker"] = symbol

            # Add time of retrieval to dataframe
            df["tdate"] = datetime.now(tz=timezone.utc).replace(hour=20, minute=0, second=0, microsecond=0) \
                if datetime.now(tz=timezone.utc).time() > time(20, 0, 0) else datetime.now(tz=timezone.utc)

            # wait for rate-limiting
            time.sleep(0.49)
        except Exception:
            traceback.print_exc()
            time.sleep(1)
            continue
        break
    return df


# Get options data and save to a database
def get_data(symbols, c):
    engine = create_engine(config.psql)
    df = pd.DataFrame()
    for symbol in symbols:
        result = options_chain(symbol, c)
        try:
            df = pd.concat([df, result])
            df.to_sql('optionsdata', engine, if_exists='append', index=False)
            del df
        except Exception:
            traceback.print_exc()
    return None


def main():
    c = loginTDA()
    print(datetime.now(tz=timezone.utc).strftime("%H:%M:%S"), "Welcome, good luck and have fun!")
    while True:
        symbols = getsymbols()
        market_open = OpenCheck()  # Check if markets are open
        while market_open == True:
                market_open = OpenCheck()  # Check if markets are still open
                print(datetime.now(tz=timezone.utc).strftime("%H:%M:%S"), "Markets are open!")
                while market_open == True:
                    market_open = OpenCheck()  # Check if markets are still open
                    print(symbols, len(symbols))
                    if market_open == True:
                        try:
                            get_data(symbols, c)
                        except Exception:
                            traceback.print_exc()
                    else:
                        print(datetime.now(tz=timezone.utc).strftime("%H:%M:%S"), "Markets are closed, have a nice day!")
                        break
                else:
                    print(datetime.now(tz=timezone.utc).strftime("%H:%M:%S"), "Markets are closed, have a nice day!")
                    break
        else:
            time.sleep(1)  # Sleep until the markets are open again
        continue
    return None

if __name__ == "__main__":
    main()

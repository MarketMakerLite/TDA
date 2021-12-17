"""--------------------------------------------------------------------------------------------------------------------
Copyright 2021 Market Maker Lite, LLC (MML) & Kyk_n_wyng
Licensed under the Apache License, Version 2.0
THIS CODE IS PROVIDED AS IS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND
This file is part of the MML Open Source Library (www.github.com/MarketMakerLite)
--------------------------------------------------------------------------------------------------------------------"""
import json
import pandas as pd
from tda import auth, client

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

def account_details():
    # Get Orders from TDA
    fields = c.Account.Fields('orders')
    response = c.get_account(config.account_id, fields = fields)
    r = json.load(response)
    
    # Unnest JSON response to dict with the order_id, datetime, underlying, effect, symbol, quantity, status
    pos = {}
    for order_strat in r["securitiesAccount"]['orderStrategies']:
      
        # Get values from orderStrategies
        status = order_strat['status']
        order_id = order_strat['orderId']
        time_value = order_strat['enteredTime']
        quantity = order_strat["quantity"]
        
        # Get values from Leg orderLegCollection
        for legs in order_strat['orderLegCollection']:
            effect_value = legs['positionEffect']
            inst = legs['instrument']
            opt_symbol = inst['symbol']
            usymbol_value = inst['underlyingSymbol']
            
            # Create a dict
            pos[order_id] = [time_value, usymbol_value, effect_value, opt_symbol, quantity, status]
            
    # Print dict
    print(pos)
    
    # Return a Dataframe
    df = pd.DataFrame(pos).T.reset_index()
    df.columns = ['order_id', 'datetime', 'underlying', 'effect', 'symbol', 'quantity', 'status']
    
    # Print Dataframe
    print(df)
   
  
if __name__ == '__main__':
  account_details()
    

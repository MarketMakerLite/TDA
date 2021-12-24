from tda.auth import easy_client
from tda.orders.options import OptionSymbol
import tda.orders.options
import datetime
import time
import math

####################################################### Login to TDA ##################################################
c = easy_client(
        api_key='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
        redirect_uri='https://localhost',
        token_path='ameritrade-credentials.json')

# Get Account ID
account_id = c.get_accounts().json()[0]['securitiesAccount']['accountId']
print(f"Logged in with account: {account_id}")

####################################################### Set Variables #################################################
symbol = 'TSLA'
expiration_date = datetime.date(year=2022, month=1, day=21)
contract_type = 'C'
strike_price = str(1000)
trigger_price = 980
trailing_stop = 3.00
quantity = 1
option_symbol = OptionSymbol(symbol, expiration_date, contract_type, strike_price).build()

##################################################### Begin Loop ######################################################

while True:
    # Get Underlying Price
    print("Getting underlying price...")
    price = c.get_quotes(symbol).json()
    price = price[f'{symbol}']['lastPrice']
    print(f'{symbol} price: {price}')

    if price <= trigger_price:
        print(f"{symbol} price under trigger price, placing order...")
        # Get Option Price
        option_price = c.get_quotes(option_symbol).json()
        # Unpack JSON
        option_price = option_price[f'{option_symbol}']['mark']
        # Round up to nearest 0.05
        option_price = math.ceil(option_price * 20)/20

        # Build order
        resp = tda.orders.options.option_buy_to_open_limit(symbol=option_symbol, quantity=quantity, price=option_price)\
        .set_stop_price_offset(trailing_stop).set_stop_price_link_basis(tda.orders.common.StopPriceLinkBasis.MARK)\
        .set_stop_price_link_type(tda.orders.common.StopPriceLinkType.VALUE)

        #Print Order Details
        print(f'Order details: \n{resp.build()}')

        # Place Order
        order_resp = c.place_order(account_id, resp.build())

        # Print Response
        if order_resp.status_code == 201:
            print("Your order was placed successfully")
        elif order_resp.status_code == 401:
            print("Validation problem with the request.")
        elif order_resp.status_code == 500:
            print("Unexpected server error.")
        elif order_resp.status_code == 403:
            print("You are forbidden from accessing this page.")
        else:
            print('Unknown Error')
        print("Exiting")
        break
    else:
        # Loop
        wait = 10
        print(f'{symbol} price > {trigger_price}, waiting {wait} seconds before next loop')
        time.sleep(wait)

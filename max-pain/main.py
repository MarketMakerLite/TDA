from tda.auth import easy_client
import pandas as pd
import datetime
import plotly.express as px
from dateutil import tz

def total_loss_at_strike(call_df, put_df, expiry_price):
    """Calculate loss at strike price"""
    # All call options with strike price below the expiry price will result in loss for option writers
    in_money_calls = call_df[call_df['strikePrice'] < expiry_price][["openInterest", "strikePrice"]]
    in_money_calls["CE loss"] = (expiry_price - in_money_calls['strikePrice'])*in_money_calls["openInterest"]

    # All put options with strike price above the expiry price will result in loss for option writers
    in_money_puts = put_df[put_df['strikePrice'] > expiry_price][["openInterest", "strikePrice"]]
    in_money_puts["PE loss"] = (in_money_puts['strikePrice'] - expiry_price)*in_money_puts["openInterest"]
    total_loss = in_money_calls["CE loss"].sum() + in_money_puts["PE loss"].sum()
    return total_loss


####################################################### Login to TDA ##################################################
c = easy_client(
        api_key='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
        redirect_uri='https://localhost',
        token_path='ameritrade-credentials.json')

####################################################### Set Variables #################################################
symbol = 'TSLA'
expiry = '2022-01-21'  # YYYY-MM-DD

####################################################### Retrieve Data #################################################
options_dict = []
query = c.get_option_chain(symbol).json()
# Flatten nested JSON
for contr_type in ['callExpDateMap', 'putExpDateMap']:
    contract = dict(query)[contr_type]
    expirations = contract.keys()
    for expirys in list(expirations):
        strikes = contract[expirys].keys()
        for st in list(strikes):
            entry = contract[expirys][st][0]
            options_dict.append(entry)
# Convert dictionary to dataframe
df = pd.DataFrame(options_dict)

# Filter expiration date
expiry = datetime.datetime.strptime(expiry, "%Y-%m-%d")
expiry = int(datetime.datetime.timestamp(expiry.replace(hour=21, tzinfo=tz.UTC))) * 1000
df = df[df['expirationDate'] == expiry]

################################################## Calculate Max Pain #################################################
call_df = df[df['putCall'] == 'CALL']
put_df = df[df['putCall'] == 'PUT']
strikes = list(df['strikePrice'])
losses = [total_loss_at_strike(call_df, put_df, strike)/1000000 for strike in strikes]
m = losses.index(min(losses))
max_pain = strikes[m]

################################################## Create Plotly Chart ################################################
colors = {'CALL': '#00c805',
          'PUT': '#FF0060'}
chart_expiry = datetime.datetime.utcfromtimestamp(df['expirationDate'][0]/1000).replace(tzinfo=tz.UTC).astimezone(tz.gettz('America/New_York')).date()
max_pain_text = f'${max_pain: ,.2f}'

fig = px.bar(df, x='strikePrice', y='openInterest', title=f"{symbol}    Expiry: {chart_expiry}    Max Pain:${max_pain}",
             template='plotly_dark', color='putCall', color_discrete_map=colors, barmode='stack',
             labels={
                  "openInterest": "Open Interest",
                  "strikePrice": "Strike Price"})

fig.add_vline(x=max_pain, line_width=3, line_color="White", annotation_text=max_pain_text)
fig.update_layout(xaxis_tickformat=',.1f', xaxis_autorange=False, xaxis_dtick=5,
                  xaxis_fixedrange=False, xaxis_tickangle=45, showlegend=False)
axis_low = max_pain * 0.8
axis_high = max_pain * 1.2
fig.update_xaxes(range=[axis_low, axis_high], row=1, col=1)

# Display Chart
fig.show()

# Market Maker Lite TDA Repository
<!-- 
[![Website](https://cldup.com/dTxpPi9lDf.thumb.png)](https://nodesource.com/products/nsolid)
-->
[![Python Badge](https://img.shields.io/badge/Python-v3.8-blue)]()
[![GitHub license](https://badgen.net/github/license/MarketMakerLite/TDA)](https://github.com/MarketMakerLite/TDA/blob/master/LICENSE)
[![GitHub last commit](https://img.shields.io/github/last-commit/MarketMakerLite/TDA)](https://github.com/MarketMakerLite/TDA/commits/main)
[![Discord](https://img.shields.io/discord/837528551028817930?color=%237289DA&label=Discord)](https://discord.gg/jjDcZcqXWy)

A repository of code that interacts with the TDA-API

### Options-Data
This code loops through a list of tickers to get the entire options chain for each symbol.
It's designed to run all day, and will consistently loop through the list of tickers. 
It will automatically handle the start and end of the trading day, as well as holidays, etc. 

### Get-Orders
This code gets all orders for a specific account, the result can then be filtered for specific orders.
There's an option for returning a dictionary or Dataframe response.

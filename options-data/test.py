import QuantLib as ql
import datetime
from datetime import timezone, timedelta
import pandas_market_calendars as mcal

# cal = ql.UnitedStates(ql.UnitedStates.NYSE)
# today = datetime.date.today()
# qlday = ql.Date(today.day, today.month, today.year)
# cal.isBusinessDay(qlday)

# now = datetime.datetime.now(tz=timezone.utc) - timedelta(days=3, hours=5)
# today = datetime.date.today() - timedelta(days=3)  # get today's date
# print(now, today)
# nyse = mcal.get_calendar('NYSE')
# tradingDay = mcal.get_calendar('NYSE').schedule(start_date=today, end_date=today)
# try:
#     Mopen = tradingDay.iloc[0][0]
#     Mclose = tradingDay.iloc[0][1]
#     if Mclose > now > Mopen:
#         market_open = True
#     else:
#         market_open = False
# except Exception as exe:
#     market_open = False
# print(market_open)

print(datetime.datetime.now())

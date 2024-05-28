import pandas as pd
import datetime

def setupCrawlerDateSeries(size):
  return pd.to_datetime([datetime.date.today().strftime("%Y%m%d")] * size, format='%Y%m%d')
import datetime
import requests
import pandas as pd
from io import StringIO

def fetchContractBaseInfo():
  current_dateTime = datetime.datetime.now()
  url = (
    "http://www.cffex.com.cn/sj/jycs/"
      + str(current_dateTime.year)
      + (str(current_dateTime.month) if current_dateTime.month > 9 else "0" + str(current_dateTime.month))
      + "/"
      + str(current_dateTime.day)
      + "/index.xml"
    )
  
  header = {
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
      "Accept-Encoding": "gzip, deflate, br",
      "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
      "Cache-Control": "max-age=0",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
  }
  res = requests.get(url, headers=header)
  res.close()

  xml_result = pd.read_xml(StringIO(res.text))
  # filter out the index futures
  futures_filtered_result = xml_result[xml_result['INSTRUMENT_ID'].str.contains('-') == False]
  futures_filtered_result = futures_filtered_result.reset_index(drop=True)
  # format string to date 
  futures_filtered_result['OPEN_DATE'] = pd.to_datetime(futures_filtered_result['OPEN_DATE'], format='%Y%m%d').asytype('datetime64[ns]')
  futures_filtered_result['END_TRADING_DAY'] = pd.to_datetime(futures_filtered_result['END_TRADING_DAY'], format='%Y%m%d').asytype('datetime64[ns]')
  futures_df = pd.DataFrame({'instrumentId': futures_filtered_result['INSTRUMENT_ID'], 
                        'exchange': 'CFFEX',
                        'openDate': futures_filtered_result['OPEN_DATE'],
                        'expireDate': futures_filtered_result['END_TRADING_DAY'],
                        'startDeliveryDate': pd.NaT,
                        'endDeliveryDate': pd.NaT,
                        'basisPrice': futures_filtered_result['BASIS_PRICE'],
                        'varietyType': 0,
                      })

  # filter out the index option
  option_filtered_result = xml_result[xml_result['INSTRUMENT_ID'].str.contains('-') == True]
  option_filtered_result = option_filtered_result.reset_index(drop=True)
  # format string to date
  option_filtered_result['OPEN_DATE'] = pd.to_datetime(option_filtered_result['OPEN_DATE'], format='%Y%m%d').asytype('datetime64[ns]')
  option_filtered_result['END_TRADING_DAY'] = pd.to_datetime(option_filtered_result['END_TRADING_DAY'], format='%Y%m%d').asytype('datetime64[ns]')
  option_df = pd.DataFrame({'instrumentId': option_filtered_result['INSTRUMENT_ID'], 
                        'exchange': 'CFFEX',
                        'openDate': option_filtered_result['OPEN_DATE'],
                        'expireDate': option_filtered_result['END_TRADING_DAY'],
                        'startDeliveryDate': pd.NaT,
                        'endDeliveryDate': pd.NaT,
                        'basisPrice': option_filtered_result['BASIS_PRICE'],
                        'varietyType': 1,
                      })

  final_df = pd.concat([futures_df, option_df], ignore_index=True)
  return pd.DataFrame(final_df, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])
import datetime
import requests
import pandas as pd
import random
import dolphindb as ddb
from io import StringIO

def cffex():
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
  futures_filtered_result['OPEN_DATE'] = pd.to_datetime(futures_filtered_result['OPEN_DATE'], format='%Y%m%d')
  futures_filtered_result['END_TRADING_DAY'] = pd.to_datetime(futures_filtered_result['END_TRADING_DAY'], format='%Y%m%d')
  futures_df = pd.DataFrame({'instrumentId': futures_filtered_result['INSTRUMENT_ID'], 
                        'exchange': 'CFFEX',
                        'openDate': futures_filtered_result['OPEN_DATE'],
                        'expireDate': futures_filtered_result['END_TRADING_DAY'],
                        'startDeliveryDate': None,
                        'endDeliveryDate': None,
                        'basisPrice': futures_filtered_result['BASIS_PRICE'],
                        'varietyType': 0,
                      })

  # filter out the index option
  option_filtered_result = xml_result[xml_result['INSTRUMENT_ID'].str.contains('-') == True]
  option_filtered_result = option_filtered_result.reset_index(drop=True)
  # format string to date
  option_filtered_result['OPEN_DATE'] = pd.to_datetime(option_filtered_result['OPEN_DATE'], format='%Y%m%d')
  option_filtered_result['END_TRADING_DAY'] = pd.to_datetime(option_filtered_result['END_TRADING_DAY'], format='%Y%m%d')
  option_df = pd.DataFrame({'instrumentId': option_filtered_result['INSTRUMENT_ID'], 
                        'exchange': 'CFFEX',
                        'openDate': option_filtered_result['OPEN_DATE'],
                        'expireDate': option_filtered_result['END_TRADING_DAY'],
                        'startDeliveryDate': None,
                        'endDeliveryDate': None,
                        'basisPrice': option_filtered_result['BASIS_PRICE'],
                        'varietyType': 1,
                      })

  final_df = pd.concat([futures_df, option_df], ignore_index=True)
  return pd.DataFrame(final_df, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])

def czce(url, varietyType):
    header = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Cache-Control": "max-age=0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    }
    res = requests.get(url, headers=header)
    res.close()

    json_result = pd.read_xml(StringIO(res.text))
    json_result["FrstTrdDt"] = json_result['FrstTrdDt'].replace("n.a.", None)
    json_result["LstTrdDt"] = json_result['LstTrdDt'].replace("n.a.", None)
    if varietyType == 0:
      json_result["LstDlvryDt"] = json_result['LstDlvryDt'].replace("n.a.", None)

    # format string to date 
    json_result['FrstTrdDt'] = pd.to_datetime(json_result['FrstTrdDt'], format='%Y-%m-%d')
    json_result['LstTrdDt'] = pd.to_datetime(json_result['LstTrdDt'], format='%Y-%m-%d')
    if varietyType == 0:
      json_result['LstDlvryDt'] = pd.to_datetime(json_result['LstDlvryDt'], format='%Y-%m-%d')

    return pd.DataFrame({'instrumentId': json_result['CtrCd'], 
                        'exchange': 'CZCE',
                        'openDate': json_result['FrstTrdDt'],
                        'expireDate': json_result['LstTrdDt'],
                        'startDeliveryDate': None,
                        'endDeliveryDate': json_result['LstDlvryDt'] if varietyType == 0 else None,
                        'basisPrice': None,
                        'varietyType': varietyType,
                    })

def dce(url, varietyType):
    header = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Cache-Control": "max-age=0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    }
    res = requests.post(url, data={'contractInformation.variety': 'all', 'contractInformation.trade_type': varietyType}, headers=header)
    res.close()

    json_result = pd.read_html(StringIO(res.text))
    
    json_result = json_result[0]
    # format string to date 
    json_result['开始交易日'] = pd.to_datetime(json_result['开始交易日'], format='%Y%m%d')
    json_result['最后交易日'] = pd.to_datetime(json_result['最后交易日'], format='%Y%m%d')
    if varietyType == 0:
        json_result['最后交割日'] = pd.to_datetime(json_result['最后交割日'], format='%Y%m%d')
        
    return pd.DataFrame({'instrumentId': json_result['合约代码'], 
                        'exchange': 'DCE',
                        'openDate': json_result['开始交易日'],
                        'expireDate': json_result['最后交易日'],
                        'startDeliveryDate': None,
                        'endDeliveryDate': json_result['最后交割日'] if varietyType == 0 else None,
                        'basisPrice': None,
                        'varietyType': varietyType,
                    })


def gfex(url, varietyType):
    header = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Cache-Control": "max-age=0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    }
    res = requests.post(url, data={'variety': '', 'trade_type': varietyType}, headers=header)
    res.close()

    json_result = pd.read_json(StringIO(res.text), lines=True, orient="columns")
    tmp = pd.array(json_result["data"][0])
    variables = list(tmp[0].keys())
    df_result = pd.DataFrame([[i[j] for j in variables] for i in tmp], columns=variables)
    # format string to date 
    df_result['startTradeDate'] = pd.to_datetime(df_result['startTradeDate'], format='%Y%m%d')
    df_result['endTradeDate'] = pd.to_datetime(df_result['endTradeDate'], format='%Y%m%d')
    if varietyType == 0:
      df_result['endDeliveryDate0'] = pd.to_datetime(df_result['endDeliveryDate0'], format='%Y%m%d') if varietyType == 0 else None

    return pd.DataFrame({'instrumentId': df_result['contractId'], 
                        'exchange': 'GFEX',
                        'openDate': df_result['startTradeDate'],
                        'expireDate': df_result['endTradeDate'],
                        'startDeliveryDate': None,
                        'endDeliveryDate': df_result['endDeliveryDate0'] if varietyType == 0 else None,
                        'basisPrice': None,
                        'varietyType': varietyType,
                    })

def ine(url, varietyType):
    _url = (
        url
        + datetime.date.today().strftime("%Y%m%d")
        + ".dat?rnd="
        + str(random.random())
    )

    header = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Cache-Control": "max-age=0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    }
    res = requests.get(_url, headers=header)
    res.close()

    json_result = pd.read_json(StringIO(res.text))
    tmp = pd.array(json_result["ContractBaseInfo" if varietyType == 0 else "OptionContractBaseInfo"])
    variables = list(tmp[0].keys())
    df_result = pd.DataFrame([[i[j] for j in variables] for i in tmp], columns=variables)
    # format string to date 
    df_result['OPENDATE'] = pd.to_datetime(df_result['OPENDATE'], format='%Y%m%d')
    df_result['EXPIREDATE'] = pd.to_datetime(df_result['EXPIREDATE'], format='%Y%m%d')
    if varietyType == 0:
      df_result['STARTDELIVDATE'] = pd.to_datetime(df_result['STARTDELIVDATE'], format='%Y%m%d')
      df_result['ENDDELIVDATE'] = pd.to_datetime(df_result['ENDDELIVDATE'], format='%Y%m%d')

    return pd.DataFrame({'instrumentId': df_result['INSTRUMENTID'], 
                        'exchange': 'INE',
                        'openDate': df_result['OPENDATE'],
                        'expireDate': df_result['EXPIREDATE'],
                        'startDeliveryDate': df_result['STARTDELIVDATE'] if varietyType == 0 else None,
                        'endDeliveryDate': df_result['ENDDELIVDATE'] if varietyType == 0 else None,
                        'basisPrice': df_result['BASISPRICE'] if varietyType == 0 else None,
                        'varietyType': varietyType,
                    })

def shfe(url, varietyType):
    _url = (
        url
        + datetime.date.today().strftime("%Y%m%d")
        + ".dat?params="
        + str(datetime.datetime.now().timestamp())
    )

    header = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Cache-Control": "max-age=0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    }
    res = requests.get(_url, headers=header)
    res.close()

    json_result = pd.read_json(StringIO(res.text))
    tmp = pd.array(json_result["ContractBaseInfo" if varietyType == 0 else "OptionContractBaseInfo"])
    variables = list(tmp[0].keys())
    df_result = pd.DataFrame([[i[j] for j in variables] for i in tmp], columns=variables)
    # format string to date 
    df_result['OPENDATE'] = pd.to_datetime(df_result['OPENDATE'], format='%Y%m%d')
    df_result['EXPIREDATE'] = pd.to_datetime(df_result['EXPIREDATE'], format='%Y%m%d')
    if varietyType == 0:
      df_result['STARTDELIVDATE'] = pd.to_datetime(df_result['STARTDELIVDATE'], format='%Y%m%d')
      df_result['ENDDELIVDATE'] = pd.to_datetime(df_result['ENDDELIVDATE'], format='%Y%m%d')
      # format string to float
      df_result['BASISPRICE'] = pd.to_numeric(df_result['BASISPRICE'])

    
    return pd.DataFrame({'instrumentId': df_result['INSTRUMENTID'], 
                        'exchange': 'SHFE',
                        'openDate': df_result['OPENDATE'],
                        'expireDate': df_result['EXPIREDATE'],
                        'startDeliveryDate': df_result['STARTDELIVDATE'] if varietyType == 0 else None,
                        'endDeliveryDate': df_result['ENDDELIVDATE'] if varietyType == 0 else None,
                        'basisPrice': df_result['BASISPRICE'] if varietyType == 0 else None,
                        'varietyType': varietyType,
                    })

ddb_pool = ddb.DBConnectionPool("192.168.56.105", 8902, 3, "admin", "123456")

append_handler = ddb.PartitionedTableAppender(dbPath = "dfs://htzq_base", tableName = "crawler_contract_info", partitionColName = "instrumentId", dbConnectionPool = ddb_pool)

try:
    ine_futures = ine("https://www.ine.cn/data/instrument/ContractBaseInfo", 0)
    ine_option = ine("https://www.ine.cn/data/instrument/option/ContractBaseInfo", 1)
    ine_final = pd.concat([ine_futures, ine_option], ignore_index=True)
    ine_final = pd.DataFrame(ine_final, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])
    append_result = append_handler.append(ine_final)
    print("INE: ", append_result)
except Exception as e:
    print("INE Error: ", e)

try:
    shfe_futures = shfe("https://www.shfe.com.cn/data/busiparamdata/future/ContractBaseInfo", 0)
    shfe_option = shfe("https://www.shfe.com.cn/data/busiparamdata/option/ContractBaseInfo", 1)
    shfe_final = pd.concat([shfe_futures, shfe_option], ignore_index=True)
    shfe_final = pd.DataFrame(shfe_final, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])
    append_result = append_handler.append(shfe_final)
    print("SHFE: ", append_result)
except Exception as e:
    print("SHFE Error: ", e)

try:
    gfex_futures = gfex("http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList", 0)
    gfex_option = gfex("http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList", 1)
    gfex_final = pd.concat([gfex_futures, gfex_option], ignore_index=True)
    gfex_final = pd.DataFrame(gfex_final, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])
    append_result = append_handler.append(gfex_final)
    print("GFEX: ", append_result)
except Exception as e:
    print("GFEX Error: ", e)

try:
    append_result = append_handler.append(cffex())
    print("CFFEX: ", append_result)
except Exception as e:
    print("CFFEX Error: ", e)

try:
    dce_futures = dce("http://www.dce.com.cn/publicweb/businessguidelines/queryContractInfo.html", 0)
    dce_option = dce("http://www.dce.com.cn/publicweb/businessguidelines/queryContractInfo.html", 1)
    dce_final = pd.concat([dce_futures, dce_option], ignore_index=True)
    dce_final = pd.DataFrame(dce_final, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])
    append_result = append_handler.append(dce_final)
    print("DCE: ", append_result)
except Exception as e:
    print("DCE Error: ", e) 

try:
    current_dateTime = datetime.datetime.now()
    url_params = str(current_dateTime.year) + '/' + (current_dateTime + datetime.timedelta(days=-1)).strftime("%Y%m%d")
    czce_futures = czce("http://www.czce.com.cn/cn/DFSStaticFiles/Future/" + url_params + "/FutureDataReferenceData.xml", 0)
    czce_option = czce("http://www.czce.com.cn/cn/DFSStaticFiles/Option/" + url_params + "/OptionDataReferenceData.xml", 1)
    czce_final = pd.concat([czce_futures, czce_option], ignore_index=True)
    czce_final = pd.DataFrame(czce_final, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])
    append_result = append_handler.append(czce_final)
    print("CZCE: ", append_result)
except Exception as e:
    print("CZCE Error: ", e)
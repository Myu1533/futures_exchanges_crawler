import datetime
import requests
import pandas as pd
import random
import dolphindb as ddb
from io import StringIO

REQUEST_HEADER = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Cache-Control": "max-age=0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
}

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
    
    header = REQUEST_HEADER
    res = requests.get(url, headers=header)
    res.close()

    xml_result = pd.read_xml(StringIO(res.text))
    # filter out the index futures
    futures_filtered_result = xml_result[xml_result['INSTRUMENT_ID'].str.contains('-') == False]
    futures_filtered_result = futures_filtered_result.reset_index(drop=True)
    # format string to date 
    futures_filtered_result['OPEN_DATE'] = pd.to_datetime(futures_filtered_result['OPEN_DATE'], format='%Y%m%d').astype('datetime64[ns]')
    futures_filtered_result['END_TRADING_DAY'] = pd.to_datetime(futures_filtered_result['END_TRADING_DAY'], format='%Y%m%d').astype('datetime64[ns]')
    futures_df = pd.DataFrame({'instrumentId': futures_filtered_result['INSTRUMENT_ID'].str.strip(), 
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
    option_filtered_result['OPEN_DATE'] = pd.to_datetime(option_filtered_result['OPEN_DATE'], format='%Y%m%d').astype('datetime64[ns]')
    option_filtered_result['END_TRADING_DAY'] = pd.to_datetime(option_filtered_result['END_TRADING_DAY'], format='%Y%m%d').astype('datetime64[ns]')
    option_df = pd.DataFrame({'instrumentId': option_filtered_result['INSTRUMENT_ID'].str.strip(), 
                            'exchange': 'CFFEX',
                            'openDate': option_filtered_result['OPEN_DATE'],
                            'expireDate': option_filtered_result['END_TRADING_DAY'],
                            'startDeliveryDate': pd.NaT,
                            'endDeliveryDate': pd.NaT,
                            'basisPrice': option_filtered_result['BASIS_PRICE'],
                            'varietyType': 1,
                        })
    
    return  pd.concat([futures_df, option_df], ignore_index=True)

def czce(url, varietyType):
    header = REQUEST_HEADER
    res = requests.get(url, headers=header)
    res.close()

    json_result = pd.read_xml(StringIO(res.text))
    json_result["FrstTrdDt"] = json_result['FrstTrdDt'].replace("n.a.", pd.NaT)
    json_result["LstTrdDt"] = json_result['LstTrdDt'].replace("n.a.", pd.NaT)
    if varietyType == 0:
        json_result["LstDlvryDt"] = json_result['LstDlvryDt'].replace("n.a.", pd.NaT)

    # format string to date 
    json_result['FrstTrdDt'] = json_result['FrstTrdDt'].astype('datetime64[ns]')
    json_result['LstTrdDt'] = json_result['LstTrdDt'].astype('datetime64[ns]')
    if varietyType == 0:
        json_result['LstDlvryDt'] = json_result['LstDlvryDt'].astype('datetime64[ns]')

    return pd.DataFrame({'instrumentId': json_result['CtrCd'].str.strip(), 
                        'exchange': 'CZCE',
                        'openDate': json_result['FrstTrdDt'],
                        'expireDate': json_result['LstTrdDt'],
                        'startDeliveryDate': pd.NaT,
                        'endDeliveryDate': json_result['LstDlvryDt'] if varietyType == 0 else pd.NaT,
                        'basisPrice': None,
                        'varietyType': varietyType,
                    })

def dce(url, varietyType):
    header = REQUEST_HEADER
    res = requests.post(url, data={'contractInformation.variety': 'all', 'contractInformation.trade_type': varietyType}, headers=header)
    res.close()

    json_result = pd.read_html(StringIO(res.text))
    
    json_result = json_result[0]
    # format string to date 
    json_result['开始交易日'] = pd.to_datetime(json_result['开始交易日'], format='%Y%m%d').astype('datetime64[ns]')
    json_result['最后交易日'] = pd.to_datetime(json_result['最后交易日'], format='%Y%m%d').astype('datetime64[ns]')
    if varietyType == 0:
        json_result['最后交割日'] = pd.to_datetime(json_result['最后交割日'], format='%Y%m%d').astype('datetime64[ns]')
        
    return pd.DataFrame({'instrumentId': json_result['合约代码'].str.strip(), 
                        'exchange': 'DCE',
                        'openDate': json_result['开始交易日'],
                        'expireDate': json_result['最后交易日'],
                        'startDeliveryDate': pd.NaT,
                        'endDeliveryDate': json_result['最后交割日'] if varietyType == 0 else pd.NaT,
                        'basisPrice': None,
                        'varietyType': varietyType,
                    })


def gfex(url, varietyType):
    header = REQUEST_HEADER
    res = requests.post(url, data={'variety': '', 'trade_type': varietyType}, headers=header)
    res.close()

    json_result = pd.read_json(StringIO(res.text), lines=True, orient="columns")
    tmp = pd.array(json_result["data"][0])
    variables = list(tmp[0].keys())
    df_result = pd.DataFrame([[i[j] for j in variables] for i in tmp], columns=variables)
    # format string to date 
    df_result['startTradeDate'] = df_result['startTradeDate'].astype('datetime64[ns]')
    df_result['endTradeDate'] = df_result['endTradeDate'].astype('datetime64[ns]')
    if varietyType == 0:
        df_result['endDeliveryDate0'] = df_result['endDeliveryDate0'].astype('datetime64[ns]')

    return pd.DataFrame({'instrumentId': df_result['contractId'].str.strip(), 
                        'exchange': 'GFEX',
                        'openDate': df_result['startTradeDate'],
                        'expireDate': df_result['endTradeDate'],
                        'startDeliveryDate': pd.NaT,
                        'endDeliveryDate': df_result['endDeliveryDate0'] if varietyType == 0 else pd.NaT,
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

    header = REQUEST_HEADER
    res = requests.get(_url, headers=header)
    res.close()

    json_result = pd.read_json(StringIO(res.text))
    tmp = pd.array(json_result["ContractBaseInfo" if varietyType == 0 else "OptionContractBaseInfo"])
    variables = list(tmp[0].keys())
    df_result = pd.DataFrame([[i[j] for j in variables] for i in tmp], columns=variables)
    # format string to date 
    df_result['OPENDATE'] = df_result['OPENDATE'].astype('datetime64[ns]')
    df_result['EXPIREDATE'] = df_result['EXPIREDATE'].astype('datetime64[ns]')
    if varietyType == 0:
        df_result['STARTDELIVDATE'] = df_result['STARTDELIVDATE'].astype('datetime64[ns]')
        df_result['ENDDELIVDATE'] = df_result['ENDDELIVDATE'].astype('datetime64[ns]')

    return pd.DataFrame({'instrumentId': df_result['INSTRUMENTID'].str.strip(), 
                        'exchange': 'INE',
                        'openDate': df_result['OPENDATE'],
                        'expireDate': df_result['EXPIREDATE'],
                        'startDeliveryDate': df_result['STARTDELIVDATE'] if varietyType == 0 else pd.NaT,
                        'endDeliveryDate': df_result['ENDDELIVDATE'] if varietyType == 0 else pd.NaT,
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

    header = REQUEST_HEADER
    res = requests.get(_url, headers=header)
    res.close()

    json_result = pd.read_json(StringIO(res.text))
    tmp = pd.array(json_result["ContractBaseInfo" if varietyType == 0 else "OptionContractBaseInfo"])
    variables = list(tmp[0].keys())
    df_result = pd.DataFrame([[i[j] for j in variables] for i in tmp], columns=variables)
    # format string to date 
    df_result['OPENDATE'] = df_result['OPENDATE'].astype('datetime64[ns]')
    df_result['EXPIREDATE'] = df_result['EXPIREDATE'].astype('datetime64[ns]')
    if varietyType == 0:
        df_result['STARTDELIVDATE'] = df_result['STARTDELIVDATE'].astype('datetime64[ns]')
        df_result['ENDDELIVDATE'] = df_result['ENDDELIVDATE'].astype('datetime64[ns]')
        # format string to float
        df_result['BASISPRICE'] = pd.to_numeric(df_result['BASISPRICE'])
    
    return pd.DataFrame({'instrumentId': df_result['INSTRUMENTID'].str.strip(), 
                        'exchange': 'SHFE',
                        'openDate': df_result['OPENDATE'],
                        'expireDate': df_result['EXPIREDATE'],
                        'startDeliveryDate': df_result['STARTDELIVDATE'] if varietyType == 0 else pd.NaT,
                        'endDeliveryDate': df_result['ENDDELIVDATE'] if varietyType == 0 else pd.NaT,
                        'basisPrice': df_result['BASISPRICE'] if varietyType == 0 else None,
                        'varietyType': varietyType,
                    })

if __name__ == "__main__":
    ddb_pool = ddb.DBConnectionPool("192.168.56.103", 8902, 3, "admin", "123456")

    append_handler = ddb.PartitionedTableAppender(dbPath = "dfs://htzq_base", tableName = "crawler_contract_info", partitionColName = "instrumentId", dbConnectionPool = ddb_pool)

    try:
        ine_futures = ine("https://www.ine.cn/data/instrument/ContractBaseInfo", 0)
        ine_option = ine("https://www.ine.cn/data/instrument/option/ContractBaseInfo", 1)
        ine_final = pd.concat([ine_futures, ine_option], ignore_index=True)
        append_result = append_handler.append(ine_final)
        print("INE: ", append_result)
    except Exception as e:
        print("INE Error: ", e)

    try:
        shfe_futures = shfe("https://www.shfe.com.cn/data/busiparamdata/future/ContractBaseInfo", 0)
        shfe_option = shfe("https://www.shfe.com.cn/data/busiparamdata/option/ContractBaseInfo", 1)
        shfe_final = pd.concat([shfe_futures, shfe_option], ignore_index=True)
        append_result = append_handler.append(shfe_final)
        print("SHFE: ", append_result)
    except Exception as e:
        print("SHFE Error: ", e)

    try:
        gfex_futures = gfex("http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList", 0)
        gfex_option = gfex("http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList", 1)
        gfex_final = pd.concat([gfex_futures, gfex_option], ignore_index=True)
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
        append_result = append_handler.append(czce_final)
        print("CZCE: ", append_result)
    except Exception as e:
        print("CZCE Error: ", e)
import requests
import pandas as pd
from io import StringIO

def handleGFEXContract(url, varietyType):
    url = "http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList"

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
    df_result['endDeliveryDate0'] = pd.to_datetime(df_result['endDeliveryDate0'], format='%Y%m%d')

    return pd.DataFrame({'instrumentId': df_result['contractId'], 
                        'exchange': 'GFEX',
                        'openDate': df_result['startTradeDate'],
                        'expireDate': df_result['endTradeDate'],
                        'startDeliveryDate': None,
                        'endDeliveryDate': df_result['endDeliveryDate0'] if varietyType == 0 else None,
                        'basisPrice': None,
                        'varietyType': varietyType,
                    })

def fetchContractBaseInfo():
    futures_df = handleGFEXContract("http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList", 0)
    option_df = handleGFEXContract("http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList", 1)
    final_df = pd.concat([futures_df, option_df], ignore_index=True)
    return pd.DataFrame(final_df, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])
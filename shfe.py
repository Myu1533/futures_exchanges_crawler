import requests
import datetime
import pandas as pd
from io import StringIO

def handleContract(url, varietyType):
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
    df_result['OPENDATE'] = pd.to_datetime(df_result['OPENDATE'], format='%Y%m%d').asytype('datetime64[ns]')
    df_result['EXPIREDATE'] = pd.to_datetime(df_result['EXPIREDATE'], format='%Y%m%d').asytype('datetime64[ns]')
    if varietyType == 0:
        df_result['STARTDELIVDATE'] = pd.to_datetime(df_result['STARTDELIVDATE'], format='%Y%m%d').asytype('datetime64[ns]')
        df_result['ENDDELIVDATE'] = pd.to_datetime(df_result['ENDDELIVDATE'], format='%Y%m%d').asytype('datetime64[ns]')
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

def fetchContractBaseInfo():
    futures_df = handleContract("https://www.shfe.com.cn/data/busiparamdata/future/ContractBaseInfo", 0)
    option_df = handleContract("https://www.shfe.com.cn/data/busiparamdata/option/ContractBaseInfo", 1)
    final_df = pd.concat([futures_df, option_df], ignore_index=True)
    return pd.DataFrame(final_df, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])
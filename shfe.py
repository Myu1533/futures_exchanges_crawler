import requests
import datetime
import pandas as pd
from io import StringIO

def fetchContractBaseInfo():
    url = (
        "https://www.shfe.com.cn/data/busiparamdata/future/ContractBaseInfo"
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
    res = requests.get(url, headers=header)
    res.close()

    json_result = pd.read_json(StringIO(res.text))
    tmp = pd.array(json_result["ContractBaseInfo"])
    variables = list(tmp[0].keys())
    df_result = pd.DataFrame([[i[j] for j in variables] for i in tmp], columns=variables)
    # format string to date 
    df_result['OPENDATE'] = pd.to_datetime(df_result['OPENDATE'], format='%Y%m%d')
    df_result['EXPIREDATE'] = pd.to_datetime(df_result['EXPIREDATE'], format='%Y%m%d')
    df_result['STARTDELIVDATE'] = pd.to_datetime(df_result['STARTDELIVDATE'], format='%Y%m%d')
    df_result['ENDDELIVDATE'] = pd.to_datetime(df_result['ENDDELIVDATE'], format='%Y%m%d')
    # format string to float
    df_result['BASISPRICE'] = pd.to_numeric(df_result['BASISPRICE'])
    # setup the crawler date
    # crawler_date_series = setupCrawlerDateSeries(len(df_result))
  
    return pd.DataFrame({'instrumentId': df_result['INSTRUMENTID'], 
                          'exchange': 'SHFE',
                          'openDate': df_result['OPENDATE'],
                          'expireDate': df_result['EXPIREDATE'],
                          'startDeliveryDate': df_result['STARTDELIVDATE'],
                          'endDeliveryDate': df_result['ENDDELIVDATE'],
                          'basisPrice': df_result['BASISPRICE'],
                          'varietyType': 0,
                        })
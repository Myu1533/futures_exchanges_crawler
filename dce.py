import requests
import pandas as pd
from io import StringIO

def handleDCEContract(url, varietyType):
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

def fetchContractBaseInfo():
    futures_df = handleDCEContract("http://www.dce.com.cn/publicweb/businessguidelines/queryContractInfo.html", 0)
    option_df = handleDCEContract("http://www.dce.com.cn/publicweb/businessguidelines/queryContractInfo.html", 1)
    final_df = pd.concat([futures_df, option_df], ignore_index=True)
    return pd.DataFrame(final_df, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])


import datetime
import requests
import pandas as pd
from io import StringIO

def handleContract(url, varietyType):
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
    json_result["FrstTrdDt"] = json_result['FrstTrdDt'].replace("n.a.", pd.NaT)
    json_result["LstTrdDt"] = json_result['LstTrdDt'].replace("n.a.", pd.NaT)
    if varietyType == 0:
      json_result["LstDlvryDt"] = json_result['LstDlvryDt'].replace("n.a.", pd.NaT)

    # format string to date 
    json_result['FrstTrdDt'] = pd.to_datetime(json_result['FrstTrdDt'], format='%Y-%m-%d').asytype('datetime64[ns]')
    json_result['LstTrdDt'] = pd.to_datetime(json_result['LstTrdDt'], format='%Y-%m-%d').asytype('datetime64[ns]')
    if varietyType == 0:
      json_result['LstDlvryDt'] = pd.to_datetime(json_result['LstDlvryDt'], format='%Y-%m-%d').asytype('datetime64[ns]')

    return pd.DataFrame({'instrumentId': json_result['CtrCd'].str.strip(), 
                        'exchange': 'CZCE',
                        'openDate': json_result['FrstTrdDt'],
                        'expireDate': json_result['LstTrdDt'],
                        'startDeliveryDate': pd.NaT,
                        'endDeliveryDate': json_result['LstDlvryDt'] if varietyType == 0 else pd.NaT,
                        'basisPrice': None,
                        'varietyType': varietyType,
                    })

def fetchContractBaseInfo():
    current_dateTime = datetime.datetime.now()
    url_params = str(current_dateTime.year) + '/' + (current_dateTime + datetime.timedelta(days=-1)).strftime("%Y%m%d")
    futures_df = handleContract("http://www.czce.com.cn/cn/DFSStaticFiles/Future/" + url_params + "/FutureDataReferenceData.xml", 0)
    option_df = handleContract("http://www.czce.com.cn/cn/DFSStaticFiles/Option/" + url_params + "/OptionDataReferenceData.xml", 1)
    final_df = pd.concat([futures_df, option_df], ignore_index=True)
    return pd.DataFrame(final_df, columns=['instrumentId', 'exchange', 'openDate', 'expireDate', 'startDeliveryDate', 'endDeliveryDate', 'basisPrice', 'varietyType'])


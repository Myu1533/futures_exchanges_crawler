import requests
import datetime
import pandas as pd

url = (
    "https://www.ine.cn/data/instrument/ContractBaseInfo"
    + datetime.date.today().strftime("%Y%m%d")
    + ".dat"
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

json_result = pd.read_json(res.text)
tmp = pd.array(json_result["ContractBaseInfo"])
variables = list(tmp[0].keys())
df_result = pd.DataFrame([[i[j] for j in variables] for i in tmp], columns=variables)
print(df_result)

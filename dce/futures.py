import requests
from requests_toolbelt import MultipartEncoder
from lxml import etree
import pandas as pd

url = "http://www.dce.com.cn/publicweb/businessguidelines/queryContractInfo.html"

res = requests.post(
    url,
    data=MultipartEncoder(
        fields={
            "dayTradingParameters.variety_id": "all",
            "ddayTradingParameters.trade_type": "0",
        }
    ),
)

html_elements = etree.HTML(res.text)
# html_table_xpath = html_elements.xpath("//table")
html_table = etree.tostring(
    html_elements.xpath("//table")[0], encoding="utf-8"
).decode()
print(html_table)
df = pd.read_html(html_table, encoding="utf-8", header=0)[0]

print(df)

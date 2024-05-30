## 建库

```SQL
dbName = "dfs://htzq_base"
key1 = database("", RANGE, 2023.01M..2045.12M)
key2 = database("", HASH, [SYMBOL, 3])
db = database(dbName, COMPO, [key1, key2], engine="TSDB")
```

## 建表

```SQL
db = database("dfs://htzq_base")
t = table(100:0, `instrumentId`exchange`openDate`expireDate`startDeliveryDate`endDeliveryDate`basisPrice, [SYMBOL, STRING, DATE, DATE, DATE, DATE, DOUBLE])
db.createPartitionedTable(t, `crawler_contract_info, `openDate`instrumentId, sortColumns=`instrumentId`openDate, keepDuplicates=LAST)
```

## 交易所合约参数对比

| 合约参数     | gfex | ine | cffex | czce | dce | shfe |
| ------------ | ---- | --- | ----- | ---- | --- | ---- |
| 合约代码     | 1    | 1   | 1     | 1    | 1   | 1    |
| 上市日期     | 1    | 1   | 1     | 1    | 1   | 1    |
| 到期日期     | 1    | 1   | 1     | 1    | 1   | 1    |
| 开始交割日   | 0    | 1   | 0     | 1    | 0   | 1    |
| 最后交割日   | 1    | 1   | 0     | 1    | 1   | 1    |
| 挂牌基准价   | 0    | 1   | 1     | 0    | 0   | 1    |
| 品种         | 1    | 0   | 0     | 1    | 1   | 0    |
| 交易单位     | 1    | 0   | 0     | 1    | 1   | 0    |
| 最小变动单位 | 1    | 0   | 0     | 1    | 1   | 0    |

## 上期所合约参数 采集结构

接口: https://www.shfe.com.cn/data/busiparamdata/future/ContractBaseInfo20240527.dat （**注意文件名**）

方法：GET

参数：rng 随机数

| 变量名         | 类型   | 说明       |
| -------------- | ------ | ---------- |
| INSTRUMENTID   | String | 合约代码   |
| OPENDATE       | String | 上市日     |
| EXPIREDATE     | String | 到期日     |
| STARTDELIVDATE | String | 开始交割日 |
| ENDDELIVDATE   | String | 最后交割日 |
| BASISPRICE     | Float  | 基准价     |

## 能源所合约参数 采集结构

接口: https://www.ine.cn/data/instrument/ContractBaseInfo20240527.dat （**注意文件名**）

方法：GET

参数：params 时间戳

| 变量名         | 类型   | 说明       |
| -------------- | ------ | ---------- |
| INSTRUMENTID   | String | 合约代码   |
| OPENDATE       | String | 上市日     |
| EXPIREDATE     | String | 到期日     |
| STARTDELIVDATE | String | 开始交割日 |
| ENDDELIVDATE   | String | 最后交割日 |
| BASISPRICE     | Float  | 基准价     |

**ec 欧洲集运指数合约**，上期所和能源所都会发布信息，交易处于**能源所**， 采集的时候以**能源所**信息为主。

能源所期权信息中，交易所标记为**上期所**

## 广期所合约参数 采集结构

接口: http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList

方法：POST

参数：formData([variety, trade_type])

| 变量名           | 类型   | 说明       |
| ---------------- | ------ | ---------- |
| contractId       | String | 合约代码   |
| startTradeDate   | String | 上市日     |
| endTradeDate     | String | 到期日     |
| endDeliveryDate0 | String | 最后交割日 |

## 中金所合约参数 采集结构

接口: http://www.cffex.com.cn/sj/jycs/202405/28/index.xml

方法：GET

参数：按日期区分数据，根据请求连接结构拼接

| 变量名          | 类型   | 说明     |
| --------------- | ------ | -------- |
| INSTRUMENT_ID   | String | 合约代码 |
| OPEN_DATE       | String | 上市日   |
| END_TRADING_DAY | String | 到期日   |
| BASIS_PRICE     | Float  | 基准价   |

由于中金所一个接口提供期货期权的合约，**直接处理全量数据**

## 大商所合约参数 采集结构

接口: http://www.dce.com.cn/publicweb/businessguidelines/queryContractInfo.html

方法：POST

参数：formData([contractInformation.variety, contractInformation.trade_type])

| 变量名     | 类型   | 说明       |
| ---------- | ------ | ---------- |
| 合约代码   | String | 合约代码   |
| 开始交易日 | String | 上市日     |
| 最后交易日 | String | 到期日     |
| 最后交割日 | String | 最后交割日 |

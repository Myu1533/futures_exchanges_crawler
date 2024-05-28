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
t = table(100:0, `instrumentId`exchange`openDate`expireDate`startDeliveryDate`endDeliveryDate`basisPrice`crawlerDate, [SYMBOL, STRING, DATE, DATE, DATE, DATE, DOUBLE, DATE])
db.createPartitionedTable(t, `crawler_contract_info, `crawlerDate`instrumentId, sortColumns=`instrumentId`crawlerDate, keepDuplicates=LAST)
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

## 上期所合约参数 入库结构

api: https://www.shfe.com.cn/data/busiparamdata/future/ContractBaseInfo20240527.dat

注意文件名

| 变量名         | 类型   | 说明       |
| -------------- | ------ | ---------- |
| INSTRUMENTID   | String | 合约代码   |
| OPENDATE       | String | 上市日     |
| EXPIREDATE     | String | 到期日     |
| STARTDELIVDATE | String | 开始交割日 |
| ENDDELIVDATE   | String | 最后交割日 |
| BASISPRICE     | String | 基准价     |

## 能源所合约参数 入库结构

api: https://www.ine.cn/data/instrument/ContractBaseInfo20240527.dat

注意文件名

| 变量名         | 类型   | 说明       |
| -------------- | ------ | ---------- |
| BASISPRICE     | String | 基准价     |
| ENDDELIVDATE   | String | 最后交割日 |
| EXPIREDATE     | String | 到期日     |
| INSTRUMENTID   | String | 合约代码   |
| OPENDATE       | String | 上市日     |
| STARTDELIVDATE | String | 开始交割日 |

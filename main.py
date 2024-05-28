from ine import fetchContractBaseInfo as ineContractBaseInfo
from shfe import fetchContractBaseInfo as shfeContractBaseInfo
import dolphindb as ddb

ddb_pool = ddb.DBConnectionPool("192.168.56.105", 8902, 3, "admin", "123456")

append_handler = ddb.PartitionedTableAppender(dbPath = "dfs://htzq_base", tableName = "crawler_contract_info", partitionColName = "instrumentId", dbConnectionPool = ddb_pool)

append_result = append_handler.append(ineContractBaseInfo())

print(append_result)

append_result = append_handler.append(shfeContractBaseInfo())

print(append_result)

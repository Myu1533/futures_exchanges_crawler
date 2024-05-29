from ine import fetchContractBaseInfo as ineContractBaseInfo
from shfe import fetchContractBaseInfo as shfeContractBaseInfo
from gfex import fetchContractBaseInfo as gfexContractBaseInfo
from cffex import fetchContractBaseInfo as cffexContractBaseInfo
import dolphindb as ddb

ddb_pool = ddb.DBConnectionPool("192.168.56.105", 8902, 3, "admin", "123456")

append_handler = ddb.PartitionedTableAppender(dbPath = "dfs://htzq_base", tableName = "crawler_contract_info", partitionColName = "instrumentId", dbConnectionPool = ddb_pool)

# try:
#     append_result = append_handler.append(ineContractBaseInfo())
#     print("INE: ", append_result)
# except Exception as e:
#     print("INE Error: ", e)

# try:
#     append_result = append_handler.append(shfeContractBaseInfo())
#     print("SHFE: ", append_result)
# except Exception as e:
#     print("SHFE Error: ", e)

# try:
#     append_result = append_handler.append(gfexContractBaseInfo())
#     print("GFEX: ", append_result)
# except Exception as e:
#     print("GFEX Error: ", e)

try:
    append_result = append_handler.append(cffexContractBaseInfo())
    print("CFFEX: ", append_result)
except Exception as e:
    print("CFFEX Error: ", e)

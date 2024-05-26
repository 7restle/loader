import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
import struct
import happybase

sp = SparkSession.builder.appName("loader").getOrCreate()

connection = happybase.Connection("localhost", 9090)


def ship_mode_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        if i == 0:
            if tmp[i] == "":
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        else:
            if tmp[i] == "":
                tmp[i] = None
            else:
                tmp[i] = tmp[i]
    return tmp


ship_mode_schema = StructType(
    [
        StructField("sm_ship_mode_sk", LongType(), False),
        StructField("sm_ship_mode_id", StringType(), False),
        StructField("sm_type", StringType(), True),
        StructField("sm_code", StringType(), True),
        StructField("sm_carrier", StringType(), True),
        StructField("sm_contract", StringType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(data_path + "ship_mode/ship_mode_{:02d}.dat".format(1))
all_data = sp.createDataFrame(data.map(ship_mode_filter), ship_mode_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(
        data_path + "ship_mode/ship_mode_{:02d}.dat".format(i)
    )
    all_data = all_data.union(data.map(ship_mode_filter).toDF(schema=ship_mode_schema))

table = connection.table("ship_mode")
for row in all_data.collect():
    table.put(
        bin(row["sm_ship_mode_sk"]),
        {
            "str:sm_ship_mode_id": row["sm_ship_mode_id"],
            "str:sm_type": row["sm_type"],
            "str:sm_code": row["sm_code"],
            "str:sm_carrier": row["sm_carrier"],
            "str:sm_contract": row["sm_contract"],
        },
    )

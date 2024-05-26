import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
import struct
import happybase

sp = SparkSession.builder.appName("loader").getOrCreate()

connection = happybase.Connection("localhost", 9090)


def reason_filter(data):
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


reason_schema = StructType(
    [
        StructField("r_reason_sk", LongType(), False),
        StructField("r_reason_id", StringType(), False),
        StructField("r_reason_desc", StringType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(data_path + "reason/reason_{:02d}.dat".format(1))
all_data = sp.createDataFrame(data.map(reason_filter), reason_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(data_path + "reason/reason_{:02d}.dat".format(i))
    all_data = all_data.union(data.map(reason_filter).toDF(schema=reason_schema))

table = connection.table("reason")
for row in all_data.collect():
    table.put(
        bin(row["r_reason_sk"]),
        {
            "str:r_reason_id": row["r_reason_id"],
            "str:r_reason_desc": row["r_reason_desc"],
        },
    )

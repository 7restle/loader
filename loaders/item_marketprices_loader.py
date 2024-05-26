import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
)
import struct
import happybase

sp = SparkSession.builder.appName("loader").getOrCreate()

connection = happybase.Connection("localhost", 9090)


def imp_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        if i in [0, 1, 4, 5]:
            if tmp[i] == "":
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i == 3:
            if tmp[i] == "":
                tmp[i] = 0.0
            else:
                tmp[i] = float(tmp[i])
        else:
            if tmp[i] == "":
                tmp[i] = None
            else:
                tmp[i] = tmp[i]
    return tmp


imp_schema = StructType(
    [
        StructField("imp_sk", LongType(), False),
        StructField("imp_item_sk", LongType(), False),
        StructField("imp_competitor", StringType(), True),
        StructField("imp_competitor_price", DoubleType(), True),
        StructField("imp_start_date", LongType(), True),
        StructField("imp_end_date", LongType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(data_path + "imp/imp_{:02d}.dat".format(1))
all_data = sp.createDataFrame(data.map(imp_filter), imp_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(data_path + "imp/imp_{:02d}.dat".format(i))
    all_data = all_data.union(data.map(imp_filter).toDF(schema=imp_schema))

table = connection.table("imp")
for row in all_data.collect():
    table.put(
        bin(row["imp_sk"]),
        {
            "long:imp_item_sk": struct.pack("q", row["imp_item_sk"]),
            "str:imp_competitor": row["imp_competitor"],
            "double:imp_competitor_price": struct.pack(
                "d", row["imp_competitor_price"]
            ),
            "long:imp_start_date": struct.pack("q", row["imp_start_date"]),
            "long:imp_end_date": struct.pack("q", row["imp_end_date"]),
        },
    )

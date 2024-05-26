import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import struct
import happybase

sp = SparkSession.builder.appName("loader").getOrCreate()

connection = happybase.Connection("localhost", 9090)


def time_dim_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        if i in [0, 2, 3, 4, 5]:
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


time_dim_schema = StructType(
    [
        StructField("t_time_sk", LongType(), False),
        StructField("t_time_id", StringType(), False),
        StructField("t_time", IntegerType(), False),
        StructField("t_hour", IntegerType(), True),
        StructField("t_minute", IntegerType(), True),
        StructField("t_second", IntegerType(), True),
        StructField("t_am_pm", StringType(), True),
        StructField("t_shift", StringType(), True),
        StructField("t_sub_shift", StringType(), True),
        StructField("t_meal_time", StringType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(data_path + "time_dim/time_dim_{:02d}.dat".format(1))
all_data = sp.createDataFrame(data.map(time_dim_filter), time_dim_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(
        data_path + "time_dim/time_dim_{:02d}.dat".format(i)
    )
    all_data = all_data.union(data.map(time_dim_filter).toDF(schema=time_dim_schema))

table = connection.table("time_dim")
for row in all_data.collect():
    table.put(
        bin(row["t_time_sk"]),
        {
            "str:t_time_id": row["t_time_id"],
            "int:t_time": struct.pack("i", row["t_time"]),
            "int:t_hour": struct.pack("i", row["t_hour"]),
            "int:t_minute": struct.pack("i", row["t_minute"]),
            "int:t_second": struct.pack("i", row["t_second"]),
            "str:t_am_pm": row["t_am_pm"],
            "str:t_shift": row["t_shift"],
            "str:t_sub_shift": row["t_sub_shift"],
            "str:t_meal_time": row["t_meal_time"],
        },
    )

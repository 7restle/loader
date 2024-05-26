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


def promotion_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        if i in [0, 2, 3, 4, 6]:
            if tmp[i] == "":
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i == 5:
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


promotion_schema = StructType(
    [
        StructField("p_promo_sk", LongType(), False),
        StructField("p_promo_id", StringType(), False),
        StructField("p_start_date_sk", LongType(), True),
        StructField("p_end_date_sk", LongType(), True),
        StructField("p_item_sk", LongType(), True),
        StructField("p_cost", DoubleType(), True),
        StructField("p_response_target", IntegerType(), True),
        StructField("p_promo_name", StringType(), True),
        StructField("p_channel_dmail", StringType(), True),
        StructField("p_channel_email", StringType(), True),
        StructField("p_channel_catalog", StringType(), True),
        StructField("p_channel_tv", StringType(), True),
        StructField("p_channel_radio", StringType(), True),
        StructField("p_channel_press", StringType(), True),
        StructField("p_channel_event", StringType(), True),
        StructField("p_channel_demo", StringType(), True),
        StructField("p_channel_details", StringType(), True),
        StructField("p_purpose", StringType(), True),
        StructField("p_discount_active", StringType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(data_path + "promotion/promotion_{:02d}.dat".format(1))
all_data = sp.createDataFrame(data.map(promotion_filter), promotion_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(
        data_path + "promotion/promotion_{:02d}.dat".format(i)
    )
    all_data = all_data.union(data.map(promotion_filter).toDF(schema=promotion_schema))

table = connection.table("promotion")
for row in all_data.collect():
    table.put(
        bin(row["p_promo_sk"]),
        {
            "str:p_promo_id": row["p_promo_id"],
            "long:p_start_date_sk": struct.pack("q", row["p_start_date_sk"]),
            "long:p_end_date_sk": struct.pack("q", row["p_end_date_sk"]),
            "long:p_item_sk": struct.pack("q", row["p_item_sk"]),
            "double:p_cost": struct.pack("d", row["p_cost"]),
            "int:p_response_target": struct.pack("i", row["p_response_target"]),
            "str:p_promo_name": row["p_promo_name"],
            "str:p_channel_dmail": row["p_channel_dmail"],
            "str:p_channel_email": row["p_channel_email"],
            "str:p_channel_catalog": row["p_channel_catalog"],
            "str:p_channel_tv": row["p_channel_tv"],
            "str:p_channel_radio": row["p_channel_radio"],
            "str:p_channel_press": row["p_channel_press"],
            "str:p_channel_event": row["p_channel_event"],
            "str:p_channel_demo": row["p_channel_demo"],
            "str:p_channel_details": row["p_channel_details"],
            "str:p_purpose": row["p_purpose"],
            "str:p_discount_active": row["p_discount_active"],
        },
    )

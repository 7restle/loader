import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    LongType,
    DoubleType,
)
import struct
import happybase

sp = SparkSession.builder.appName("loader").getOrCreate()

connection = happybase.Connection("localhost", 9090)


def web_page_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        if i in [0, 3, 4, 6, 7, 10, 11, 12, 13, 14]:
            if tmp[i] == "":
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i == 8:
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


web_page_schema = StructType(
    [
        StructField("wp_web_page_sk", LongType(), False),
        StructField("wp_web_page_id", StringType(), False),
        StructField("wp_rec_start_date", DateType(), True),
        StructField("wp_rec_end_date", DateType(), True),
        StructField("wp_creation_date_sk", LongType(), True),
        StructField("wp_access_date_sk", LongType(), True),
        StructField("wp_autogen_flag", StringType(), True),
        StructField("wp_customer_sk", LongType(), True),
        StructField("wp_url", StringType(), True),
        StructField("wp_type", StringType(), True),
        StructField("wp_char_count", IntegerType(), True),
        StructField("wp_link_count", IntegerType(), True),
        StructField("wp_image_count", IntegerType(), True),
        StructField("wp_max_ad_count", IntegerType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(data_path + "web_page/web_page_{:02d}.dat".format(1))
all_data = sp.createDataFrame(data.map(web_page_filter), web_page_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(
        data_path + "web_page/web_page_{:02d}.dat".format(i)
    )
    all_data = all_data.union(data.map(web_page_filter).toDF(schema=web_page_schema))

table = connection.table("web_page")
for row in all_data.collect():
    table.put(
        bin(row["wp_web_page_sk"]),
        {
            "str:wp_web_page_id": row["wp_web_page_id"],
            "date:wp_rec_start_date": (
                row["wp_rec_start_date"].isoformat() if row["wp_rec_start_date"] else ""
            ),
            "date:wp_rec_end_date": (
                row["wp_rec_end_date"].isoformat() if row["wp_rec_end_date"] else ""
            ),
            "long:wp_creation_date_sk": struct.pack("q", row["wp_creation_date_sk"]),
            "long:wp_access_date_sk": struct.pack("q", row["wp_access_date_sk"]),
            "str:wp_autogen_flag": row["wp_autogen_flag"],
            "long:wp_customer_sk": struct.pack("q", row["wp_customer_sk"]),
            "str:wp_url": row["wp_url"],
            "str:wp_type": row["wp_type"],
            "int:wp_char_count": struct.pack("i", row["wp_char_count"]),
            "int:wp_link_count": struct.pack("i", row["wp_link_count"]),
            "int:wp_image_count": struct.pack("i", row["wp_image_count"]),
            "int:wp_max_ad_count": struct.pack("i", row["wp_max_ad_count"]),
        },
    )

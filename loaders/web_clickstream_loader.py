import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import struct
import happybase

sp = SparkSession.builder.appName("loader").getOrCreate()

connection = happybase.Connection("localhost", 9090)


def web_clickstreams_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        tmp[i] = int(tmp[i])
    return tmp


web_clickstreams_schema = StructType(
    [
        StructField("wcs_click_sk", LongType(), False),
        StructField("wcs_click_date_sk", LongType(), False),
        StructField("wcs_click_time_sk", LongType(), False),
        StructField("wcs_sales_sk", LongType(), True),
        StructField("wcs_item_sk", LongType(), True),
        StructField("wcs_web_page_sk", LongType(), False),
        StructField("wcs_user_sk", LongType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(
    data_path + "web_clickstreams/web_clickstreams_{:02d}.dat".format(1)
)
all_data = sp.createDataFrame(
    data.map(web_clickstreams_filter), web_clickstreams_schema
)
for i in range(2, 81):
    data = sp.sparkContext.textFile(
        data_path + "web_clickstreams/web_clickstreams_{:02d}.dat".format(i)
    )
    all_data = all_data.union(
        data.map(web_clickstreams_filter).toDF(schema=web_clickstreams_schema)
    )

table = connection.table("web_clickstreams")
for row in all_data.collect():
    table.put(
        bin(row["wcs_click_sk"]),
        {
            "long:wcs_click_date_sk": struct.pack("q", row["wcs_click_date_sk"]),
            "long:wcs_click_time_sk": struct.pack("q", row["wcs_click_time_sk"]),
            "long:wcs_sales_sk": (
                struct.pack("q", row["wcs_sales_sk"])
                if row["wcs_sales_sk"] is not None
                else b""
            ),
            "long:wcs_item_sk": (
                struct.pack("q", row["wcs_item_sk"])
                if row["wcs_item_sk"] is not None
                else b""
            ),
            "long:wcs_web_page_sk": struct.pack("q", row["wcs_web_page_sk"]),
            "long:wcs_user_sk": (
                struct.pack("q", row["wcs_user_sk"])
                if row["wcs_user_sk"] is not None
                else b""
            ),
        },
    )

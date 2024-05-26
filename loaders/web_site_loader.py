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


def web_site_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        if i in [0, 6, 7, 8, 12, 16, 18, 19, 21]:
            if tmp[i] == "":
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i in [22, 23]:
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


web_site_schema = StructType(
    [
        StructField("web_site_sk", LongType(), False),
        StructField("web_site_id", StringType(), False),
        StructField("web_rec_start_date", DateType(), True),
        StructField("web_rec_end_date", DateType(), True),
        StructField("web_name", StringType(), True),
        StructField("web_open_date_sk", LongType(), True),
        StructField("web_close_date_sk", LongType(), True),
        StructField("web_class", StringType(), True),
        StructField("web_manager", StringType(), True),
        StructField("web_mkt_id", IntegerType(), True),
        StructField("web_mkt_class", StringType(), True),
        StructField("web_mkt_desc", StringType(), True),
        StructField("web_market_manager", StringType(), True),
        StructField("web_company_id", IntegerType(), True),
        StructField("web_company_name", StringType(), True),
        StructField("web_street_number", StringType(), True),
        StructField("web_street_name", StringType(), True),
        StructField("web_street_type", StringType(), True),
        StructField("web_suite_number", StringType(), True),
        StructField("web_city", StringType(), True),
        StructField("web_county", StringType(), True),
        StructField("web_state", StringType(), True),
        StructField("web_zip", StringType(), True),
        StructField("web_country", StringType(), True),
        StructField("web_gmt_offset", DoubleType(), True),
        StructField("web_tax_percentage", DoubleType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(data_path + "web_site/web_site_{:02d}.dat".format(1))
all_data = sp.createDataFrame(data.map(web_site_filter), web_site_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(
        data_path + "web_site/web_site_{:02d}.dat".format(i)
    )
    all_data = all_data.union(data.map(web_site_filter).toDF(schema=web_site_schema))

table = connection.table("web_site")
for row in all_data.collect():
    table.put(
        bin(row["web_site_sk"]),
        {
            "str:web_site_id": row["web_site_id"],
            "date:web_rec_start_date": (
                row["web_rec_start_date"].isoformat()
                if row["web_rec_start_date"]
                else ""
            ),
            "date:web_rec_end_date": (
                row["web_rec_end_date"].isoformat() if row["web_rec_end_date"] else ""
            ),
            "str:web_name": row["web_name"],
            "long:web_open_date_sk": struct.pack("q", row["web_open_date_sk"]),
            "long:web_close_date_sk": struct.pack("q", row["web_close_date_sk"]),
            "str:web_class": row["web_class"],
            "str:web_manager": row["web_manager"],
            "int:web_mkt_id": struct.pack("i", row["web_mkt_id"]),
            "str:web_mkt_class": row["web_mkt_class"],
            "str:web_mkt_desc": row["web_mkt_desc"],
            "str:web_market_manager": row["web_market_manager"],
            "int:web_company_id": struct.pack("i", row["web_company_id"]),
            "str:web_company_name": row["web_company_name"],
            "str:web_street_number": row["web_street_number"],
            "str:web_street_name": row["web_street_name"],
            "str:web_street_type": row["web_street_type"],
            "str:web_suite_number": row["web_suite_number"],
            "str:web_city": row["web_city"],
            "str:web_county": row["web_county"],
            "str:web_state": row["web_state"],
            "str:web_zip": row["web_zip"],
            "str:web_country": row["web_country"],
            "double:web_gmt_offset": struct.pack("d", row["web_gmt_offset"]),
            "double:web_tax_percentage": struct.pack("d", row["web_tax_percentage"]),
        },
    )

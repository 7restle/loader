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
    DateType,
)
import struct
import happybase

sp = SparkSession.builder.appName("loader").getOrCreate()

connection = happybase.Connection("localhost", 9090)


def store_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        if i in [0, 6, 9, 11, 13, 15, 17, 19, 21, 22, 23]:
            if tmp[i] == "":
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i in [24, 25]:
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


store_schema = StructType(
    [
        StructField("s_store_sk", LongType(), False),
        StructField("s_store_id", StringType(), False),
        StructField("s_rec_start_date", DateType(), True),
        StructField("s_rec_end_date", DateType(), True),
        StructField("s_closed_date_sk", LongType(), True),
        StructField("s_store_name", StringType(), True),
        StructField("s_number_employees", IntegerType(), True),
        StructField("s_floor_space", IntegerType(), True),
        StructField("s_hours", StringType(), True),
        StructField("s_manager", StringType(), True),
        StructField("s_market_id", IntegerType(), True),
        StructField("s_geography_class", StringType(), True),
        StructField("s_market_desc", StringType(), True),
        StructField("s_market_manager", StringType(), True),
        StructField("s_division_id", IntegerType(), True),
        StructField("s_division_name", StringType(), True),
        StructField("s_company_id", IntegerType(), True),
        StructField("s_company_name", StringType(), True),
        StructField("s_street_number", StringType(), True),
        StructField("s_street_name", StringType(), True),
        StructField("s_street_type", StringType(), True),
        StructField("s_suite_number", StringType(), True),
        StructField("s_city", StringType(), True),
        StructField("s_county", StringType(), True),
        StructField("s_state", StringType(), True),
        StructField("s_zip", StringType(), True),
        StructField("s_country", StringType(), True),
        StructField("s_gmt_offset", DoubleType(), True),
        StructField("s_tax_precentage", DoubleType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(data_path + "store/store_{:02d}.dat".format(1))
all_data = sp.createDataFrame(data.map(store_filter), store_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(data_path + "store/store_{:02d}.dat".format(i))
    all_data = all_data.union(data.map(store_filter).toDF(schema=store_schema))

table = connection.table("store")
for row in all_data.collect():
    table.put(
        bin(row["s_store_sk"]),
        {
            "str:s_store_id": row["s_store_id"],
            "date:s_rec_start_date": (
                row["s_rec_start_date"].isoformat() if row["s_rec_start_date"] else ""
            ),
            "date:s_rec_end_date": (
                row["s_rec_end_date"].isoformat() if row["s_rec_end_date"] else ""
            ),
            "long:s_closed_date_sk": (
                struct.pack("q", row["s_closed_date_sk"])
                if row["s_closed_date_sk"] is not None
                else b""
            ),
            "str:s_store_name": row["s_store_name"],
            "int:s_number_employees": struct.pack("i", row["s_number_employees"]),
            "int:s_floor_space": struct.pack("i", row["s_floor_space"]),
            "str:s_hours": row["s_hours"],
            "str:s_manager": row["s_manager"],
            "int:s_market_id": struct.pack("i", row["s_market_id"]),
            "str:s_geography_class": row["s_geography_class"],
            "str:s_market_desc": row["s_market_desc"],
            "str:s_market_manager": row["s_market_manager"],
            "int:s_division_id": struct.pack("i", row["s_division_id"]),
            "str:s_division_name": row["s_division_name"],
            "int:s_company_id": struct.pack("i", row["s_company_id"]),
            "str:s_company_name": row["s_company_name"],
            "str:s_street_number": row["s_street_number"],
            "str:s_street_name": row["s_street_name"],
            "str:s_street_type": row["s_street_type"],
            "str:s_suite_number": row["s_suite_number"],
            "str:s_city": row["s_city"],
            "str:s_county": row["s_county"],
            "str:s_state": row["s_state"],
            "str:s_zip": row["s_zip"],
            "str:s_country": row["s_country"],
            "double:s_gmt_offset": struct.pack("d", row["s_gmt_offset"]),
            "double:s_tax_precentage": struct.pack("d", row["s_tax_precentage"]),
        },
    )

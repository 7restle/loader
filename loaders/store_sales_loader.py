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


def store_sales_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        if i in [
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
        ]:
            if tmp[i] == "":
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i in [24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35]:
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


store_sales_schema = StructType(
    [
        StructField("ss_sold_date_sk", LongType(), True),
        StructField("ss_sold_time_sk", LongType(), True),
        StructField("ss_item_sk", LongType(), False),
        StructField("ss_customer_sk", LongType(), True),
        StructField("ss_cdemo_sk", LongType(), True),
        StructField("ss_hdemo_sk", LongType(), True),
        StructField("ss_addr_sk", LongType(), True),
        StructField("ss_store_sk", LongType(), True),
        StructField("ss_promo_sk", LongType(), True),
        StructField("ss_ticket_number", LongType(), False),
        StructField("ss_quantity", IntegerType(), True),
        StructField("ss_wholesale_cost", DoubleType(), True),
        StructField("ss_list_price", DoubleType(), True),
        StructField("ss_sales_price", DoubleType(), True),
        StructField("ss_ext_discount_amt", DoubleType(), True),
        StructField("ss_ext_sales_price", DoubleType(), True),
        StructField("ss_ext_wholesale_cost", DoubleType(), True),
        StructField("ss_ext_list_price", DoubleType(), True),
        StructField("ss_ext_tax", DoubleType(), True),
        StructField("ss_coupon_amt", DoubleType(), True),
        StructField("ss_net_paid", DoubleType(), True),
        StructField("ss_net_paid_inc_tax", DoubleType(), True),
        StructField("ss_net_profit", DoubleType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(
    data_path + "store_sales/store_sales_{:02d}.dat".format(1)
)
all_data = sp.createDataFrame(data.map(store_sales_filter), store_sales_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(
        data_path + "store_sales/store_sales_{:02d}.dat".format(i)
    )
    all_data = all_data.union(
        data.map(store_sales_filter).toDF(schema=store_sales_schema)
    )

table = connection.table("store_sales")
for row in all_data.collect():
    table.put(
        bin(row["ss_sold_date_sk"]),
        {
            "long:ss_sold_time_sk": struct.pack("q", row["ss_sold_time_sk"]),
            "long:ss_item_sk": struct.pack("q", row["ss_item_sk"]),
            "long:ss_customer_sk": struct.pack("q", row["ss_customer_sk"]),
            "long:ss_cdemo_sk": struct.pack("q", row["ss_cdemo_sk"]),
            "long:ss_hdemo_sk": struct.pack("q", row["ss_hdemo_sk"]),
            "long:ss_addr_sk": struct.pack("q", row["ss_addr_sk"]),
            "long:ss_store_sk": struct.pack("q", row["ss_store_sk"]),
            "long:ss_promo_sk": struct.pack("q", row["ss_promo_sk"]),
            "long:ss_ticket_number": struct.pack("q", row["ss_ticket_number"]),
            "int:ss_quantity": struct.pack("i", row["ss_quantity"]),
            "double:ss_wholesale_cost": struct.pack("d", row["ss_wholesale_cost"]),
            "double:ss_list_price": struct.pack("d", row["ss_list_price"]),
            "double:ss_sales_price": struct.pack("d", row["ss_sales_price"]),
            "double:ss_ext_discount_amt": struct.pack("d", row["ss_ext_discount_amt"]),
            "double:ss_ext_sales_price": struct.pack("d", row["ss_ext_sales_price"]),
            "double:ss_ext_wholesale_cost": struct.pack(
                "d", row["ss_ext_wholesale_cost"]
            ),
            "double:ss_ext_list_price": struct.pack("d", row["ss_ext_list_price"]),
            "double:ss_ext_tax": struct.pack("d", row["ss_ext_tax"]),
            "double:ss_coupon_amt": struct.pack("d", row["ss_coupon_amt"]),
            "double:ss_net_paid": struct.pack("d", row["ss_net_paid"]),
            "double:ss_net_paid_inc_tax": struct.pack("d", row["ss_net_paid_inc_tax"]),
            "double:ss_net_profit": struct.pack("d", row["ss_net_profit"]),
        },
    )

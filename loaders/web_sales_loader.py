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


def web_sales_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        if i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]:
            if tmp[i] == "":
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i in range(19, 34):
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


web_sales_schema = StructType(
    [
        StructField("ws_sold_date_sk", LongType(), False),
        StructField("ws_sold_time_sk", LongType(), False),
        StructField("ws_ship_date_sk", LongType(), False),
        StructField("ws_item_sk", LongType(), False),
        StructField("ws_bill_customer_sk", LongType(), False),
        StructField("ws_bill_cdemo_sk", LongType(), False),
        StructField("ws_bill_hdemo_sk", LongType(), False),
        StructField("ws_bill_addr_sk", LongType(), False),
        StructField("ws_ship_customer_sk", LongType(), False),
        StructField("ws_ship_cdemo_sk", LongType(), False),
        StructField("ws_ship_hdemo_sk", LongType(), False),
        StructField("ws_ship_addr_sk", LongType(), False),
        StructField("ws_web_page_sk", LongType(), False),
        StructField("ws_web_site_sk", LongType(), False),
        StructField("ws_ship_mode_sk", LongType(), False),
        StructField("ws_warehouse_sk", LongType(), False),
        StructField("ws_promo_sk", LongType(), False),
        StructField("ws_order_number", LongType(), False),
        StructField("ws_quantity", IntegerType(), False),
        StructField("ws_wholesale_cost", DoubleType(), False),
        StructField("ws_list_price", DoubleType(), False),
        StructField("ws_sales_price", DoubleType(), False),
        StructField("ws_ext_discount_amt", DoubleType(), False),
        StructField("ws_ext_sales_price", DoubleType(), False),
        StructField("ws_ext_wholesale_cost", DoubleType(), False),
        StructField("ws_ext_list_price", DoubleType(), False),
        StructField("ws_ext_tax", DoubleType(), False),
        StructField("ws_coupon_amt", DoubleType(), False),
        StructField("ws_ext_ship_cost", DoubleType(), False),
        StructField("ws_net_paid", DoubleType(), False),
        StructField("ws_net_paid_inc_tax", DoubleType(), False),
        StructField("ws_net_paid_inc_ship", DoubleType(), False),
        StructField("ws_net_paid_inc_ship_tax", DoubleType(), False),
        StructField("ws_net_profit", DoubleType(), False),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(data_path + "web_sales/web_sales_{:02d}.dat".format(1))
all_data = sp.createDataFrame(data.map(web_sales_filter), web_sales_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(
        data_path + "web_sales/web_sales_{:02d}.dat".format(i)
    )
    all_data = all_data.union(data.map(web_sales_filter).toDF(schema=web_sales_schema))

table = connection.table("web_sales")
for row in all_data.collect():
    table.put(
        bin(row["ws_order_number"]),
        {
            "long:ws_sold_date_sk": struct.pack("q", row["ws_sold_date_sk"]),
            "long:ws_sold_time_sk": struct.pack("q", row["ws_sold_time_sk"]),
            "long:ws_ship_date_sk": struct.pack("q", row["ws_ship_date_sk"]),
            "long:ws_item_sk": struct.pack("q", row["ws_item_sk"]),
            "long:ws_bill_customer_sk": struct.pack("q", row["ws_bill_customer_sk"]),
            "long:ws_bill_cdemo_sk": struct.pack("q", row["ws_bill_cdemo_sk"]),
            "long:ws_bill_hdemo_sk": struct.pack("q", row["ws_bill_hdemo_sk"]),
            "long:ws_bill_addr_sk": struct.pack("q", row["ws_bill_addr_sk"]),
            "long:ws_ship_customer_sk": struct.pack("q", row["ws_ship_customer_sk"]),
            "long:ws_ship_cdemo_sk": struct.pack("q", row["ws_ship_cdemo_sk"]),
            "long:ws_ship_hdemo_sk": struct.pack("q", row["ws_ship_hdemo_sk"]),
            "long:ws_ship_addr_sk": struct.pack("q", row["ws_ship_addr_sk"]),
            "long:ws_web_page_sk": struct.pack("q", row["ws_web_page_sk"]),
            "long:ws_web_site_sk": struct.pack("q", row["ws_web_site_sk"]),
            "long:ws_ship_mode_sk": struct.pack("q", row["ws_ship_mode_sk"]),
            "long:ws_warehouse_sk": struct.pack("q", row["ws_warehouse_sk"]),
            "long:ws_promo_sk": struct.pack("q", row["ws_promo_sk"]),
            "long:ws_order_number": struct.pack("q", row["ws_order_number"]),
            "int:ws_quantity": struct.pack("i", row["ws_quantity"]),
            "double:ws_wholesale_cost": struct.pack("d", row["ws_wholesale_cost"]),
            "double:ws_list_price": struct.pack("d", row["ws_list_price"]),
            "double:ws_sales_price": struct.pack("d", row["ws_sales_price"]),
            "double:ws_ext_discount_amt": struct.pack("d", row["ws_ext_discount_amt"]),
            "double:ws_ext_sales_price": struct.pack("d", row["ws_ext_sales_price"]),
            "double:ws_ext_wholesale_cost": struct.pack(
                "d", row["ws_ext_wholesale_cost"]
            ),
            "double:ws_ext_list_price": struct.pack("d", row["ws_ext_list_price"]),
            "double:ws_ext_tax": struct.pack("d", row["ws_ext_tax"]),
            "double:ws_coupon_amt": struct.pack("d", row["ws_coupon_amt"]),
            "double:ws_ext_ship_cost": struct.pack("d", row["ws_ext_ship_cost"]),
            "double:ws_net_paid": struct.pack("d", row["ws_net_paid"]),
            "double:ws_net_paid_inc_tax": struct.pack("d", row["ws_net_paid_inc_tax"]),
            "double:ws_net_paid_inc_ship": struct.pack(
                "d", row["ws_net_paid_inc_ship"]
            ),
            "double:ws_net_paid_inc_ship_tax": struct.pack(
                "d", row["ws_net_paid_inc_ship_tax"]
            ),
            "double:ws_net_profit": struct.pack("d", row["ws_net_profit"]),
        },
    )

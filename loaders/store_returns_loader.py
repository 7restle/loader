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


def store_returns_filter(data):
    tmp = data.split("|")
    for i in range(len(tmp)):
        if i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
            if tmp[i] == "":
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i in [10, 11, 12, 13, 14, 15, 16, 17, 18]:
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


store_returns_schema = StructType(
    [
        StructField("sr_returned_date_sk", LongType(), True),
        StructField("sr_return_time_sk", LongType(), True),
        StructField("sr_item_sk", LongType(), False),
        StructField("sr_customer_sk", LongType(), True),
        StructField("sr_cdemo_sk", LongType(), True),
        StructField("sr_hdemo_sk", LongType(), True),
        StructField("sr_addr_sk", LongType(), True),
        StructField("sr_store_sk", LongType(), True),
        StructField("sr_reason_sk", LongType(), True),
        StructField("sr_ticket_number", LongType(), False),
        StructField("sr_return_quantity", IntegerType(), True),
        StructField("sr_return_amt", DoubleType(), True),
        StructField("sr_return_tax", DoubleType(), True),
        StructField("sr_return_amt_inc_tax", DoubleType(), True),
        StructField("sr_fee", DoubleType(), True),
        StructField("sr_return_ship_cost", DoubleType(), True),
        StructField("sr_refunded_cash", DoubleType(), True),
        StructField("sr_reversed_charge", DoubleType(), True),
        StructField("sr_store_credit", DoubleType(), True),
        StructField("sr_net_loss", DoubleType(), True),
    ]
)

data_path = "hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/"
data = sp.sparkContext.textFile(
    data_path + "store_returns/store_returns_{:02d}.dat".format(1)
)
all_data = sp.createDataFrame(data.map(store_returns_filter), store_returns_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(
        data_path + "store_returns/store_returns_{:02d}.dat".format(i)
    )
    all_data = all_data.union(
        data.map(store_returns_filter).toDF(schema=store_returns_schema)
    )

table = connection.table("store_returns")
for row in all_data.collect():
    table.put(
        bin(row["sr_returned_date_sk"]),
        {
            "long:sr_return_time_sk": struct.pack("q", row["sr_return_time_sk"]),
            "long:sr_item_sk": struct.pack("q", row["sr_item_sk"]),
            "long:sr_customer_sk": struct.pack("q", row["sr_customer_sk"]),
            "long:sr_cdemo_sk": struct.pack("q", row["sr_cdemo_sk"]),
            "long:sr_hdemo_sk": struct.pack("q", row["sr_hdemo_sk"]),
            "long:sr_addr_sk": struct.pack("q", row["sr_addr_sk"]),
            "long:sr_store_sk": struct.pack("q", row["sr_store_sk"]),
            "long:sr_reason_sk": struct.pack("q", row["sr_reason_sk"]),
            "long:sr_ticket_number": struct.pack("q", row["sr_ticket_number"]),
            "int:sr_return_quantity": struct.pack("i", row["sr_return_quantity"]),
            "double:sr_return_amt": struct.pack("d", row["sr_return_amt"]),
            "double:sr_return_tax": struct.pack("d", row["sr_return_tax"]),
            "double:sr_return_amt_inc_tax": struct.pack(
                "d", row["sr_return_amt_inc_tax"]
            ),
            "double:sr_fee": struct.pack("d", row["sr_fee"]),
            "double:sr_return_ship_cost": struct.pack("d", row["sr_return_ship_cost"]),
            "double:sr_refunded_cash": struct.pack("d", row["sr_refunded_cash"]),
            "double:sr_reversed_charge": struct.pack("d", row["sr_reversed_charge"]),
            "double:sr_store_credit": struct.pack("d", row["sr_store_credit"]),
            "double:sr_net_loss": struct.pack("d", row["sr_net_loss"]),
        },
    )

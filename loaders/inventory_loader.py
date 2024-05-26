import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType
sp = SparkSession.builder.appName('loader').getOrCreate()
import struct
import happybase

connection = happybase.Connection('localhost', 9090)

def inv_filter(data):
    tmp = data.split('|')
    for i in range(len(tmp)):
        if i in [0, 1, 2, 3]:
            if tmp[i] == '':
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        else:
            if tmp[i] == '':
                tmp[i] = None
            else:
                tmp[i] = tmp[i]

    return tmp

inv_schema = StructType([
    StructField('inv_date_sk', LongType(), False),
    StructField('inv_item_sk', LongType(), False),
    StructField('inv_warehouse_sk', LongType(), True),
    StructField('inv_quantity_on_hand', IntegerType(), True)
])

data_path = 'hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/'
data = sp.sparkContext.textFile(data_path + 'inventory/inventory_{:02d}.dat'.format(1))
all_data = sp.createDataFrame(data.map(inv_filter), inv_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(data_path + 'inventory/inventory_{:02d}.dat'.format(i))
    all_data = all_data.union(data.map(inv_filter).toDF(schema=inv_schema))
table = connection.table('inventory')
a = 0
for row in all_data.collect():
    table.put(bin(a), {
        'long:inv_date_sk': struct.pack('l', row['inv_date_sk']),
        'long:inv_item_sk': struct.pack('l', row['inv_item_sk']),
        'long:inv_warehouse_sk': struct.pack('l', row['inv_warehouse_sk']),
        'int:inv_quantity_on_hand': struct.pack('i', row['inv_quantity_on_hand'])
    })
    a += 1

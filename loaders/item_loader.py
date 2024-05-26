import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType
sp = SparkSession.builder.appName('loader').getOrCreate()
import struct
import happybase

connection = happybase.Connection('localhost', 9090)

def i_filter(data):
    tmp = data.split('|')
    for i in range(len(tmp)):
        if i in [0, 7, 9, 11, 13, 19]:
            if tmp[i] == '':
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i in [5, 6]:
            if tmp[i] == '':
                tmp[i] = 0.0
            else:
                tmp[i] = float(tmp[i])
        else:
            if tmp[i] == '':
                tmp[i] = None
            else:
                tmp[i] = tmp[i]

    return tmp

i_schema = StructType([
    StructField('i_item_sk', LongType(), False),
    StructField('i_item_id', StringType(), False),
    StructField('i_rec_start_date', StringType(), True),
    StructField('i_rec_end_date', StringType(), True),
    StructField('i_item_desc', StringType(), True),
    StructField('i_current_price', FloatType(), True),
    StructField('i_wholesale_cost', FloatType(), True),
    StructField('i_brand_id', IntegerType(), True),
    StructField('i_brand', StringType(), True),
    StructField('i_class_id', IntegerType(), True),
    StructField('i_class', StringType(), True),
    StructField('i_category_id', IntegerType(), True),
    StructField('i_category', StringType(), True),
    StructField('i_manufact_id', IntegerType(), True),
    StructField('i_manufact', StringType(), True),
    StructField('i_size', StringType(), True),
    StructField('i_formulation', StringType(), True),
    StructField('i_color', StringType(), True),
    StructField('i_units', StringType(), True),
    StructField('i_container', StringType(), True),
    StructField('i_manager_id', IntegerType(), True),
    StructField('i_product_name', StringType(), True)
])

data_path = 'hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/'
data = sp.sparkContext.textFile(data_path + 'item/item_{:02d}.dat'.format(1))
all_data = sp.createDataFrame(data.map(i_filter), i_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(data_path + 'item/item_{:02d}.dat'.format(i))
    all_data = all_data.union(data.map(i_filter).toDF(schema=i_schema))
table = connection.table('item')
for row in all_data.collect():
    table.put(bin(row['i_item_sk']), {
        'str:i_item_id': row['i_item_id'],
        'str:i_rec_start_date': row['i_rec_start_date'],
        'str:i_rec_end_date': row['i_rec_end_date'],
        'str:i_item_desc': row['i_item_desc'],
        'float:i_current_price': struct.pack('f', row['i_current_price']),
        'float:i_wholesale_cost': struct.pack('f', row['i_wholesale_cost']),
        'int:i_brand_id': struct.pack('i', row['i_brand_id']),
        'str:i_brand': row['i_brand'],
        'int:i_class_id': struct.pack('i', row['i_class_id']),
        'str:i_class': row['i_class'],
        'int:i_category_id': struct.pack('i', row['i_category_id']),
        'str:i_category': row['i_category'],
        'int:i_manufact_id': struct.pack('i', row['i_manufact_id']),
        'str:i_manufact': row['i_manufact'],
        'str:i_size': row['i_size'],
        'str:i_formulation': row['i_formulation'],
        'str:i_color': row['i_color'],
        'str:i_units': row['i_units'],
        'str:i_container': row['i_container'],
        'int:i_manager_id': struct.pack('i', row['i_manager_id']),
        'str:i_product_name': row['i_product_name']
    })

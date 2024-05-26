import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
import struct
import happybase

sp = SparkSession.builder.appName('loader').getOrCreate()

connection = happybase.Connection('localhost', 9090)

def warehouse_filter(data):
    tmp = data.split('|')
    for i in range(len(tmp)):
        if i in [0, 3, 6, 9]:
            if tmp[i] == '':
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i == 13:
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

warehouse_schema = StructType([
    StructField('w_warehouse_sk', LongType(), False),
    StructField('w_warehouse_id', StringType(), False),
    StructField('w_warehouse_name', StringType(), True),
    StructField('w_warehouse_sq_ft', IntegerType(), True),
    StructField('w_street_number', StringType(), True),
    StructField('w_street_name', StringType(), True),
    StructField('w_street_type', StringType(), True),
    StructField('w_suite_number', StringType(), True),
    StructField('w_city', StringType(), True),
    StructField('w_county', StringType(), True),
    StructField('w_state', StringType(), True),
    StructField('w_zip', StringType(), True),
    StructField('w_country', StringType(), True),
    StructField('w_gmt_offset', DoubleType(), True)
])

data_path = 'hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/'
data = sp.sparkContext.textFile(data_path + 'warehouse/warehouse_{:02d}.dat'.format(1))
all_data = sp.createDataFrame(data.map(warehouse_filter), warehouse_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(data_path + 'warehouse/warehouse_{:02d}.dat'.format(i))
    all_data = all_data.union(data.map(warehouse_filter).toDF(schema=warehouse_schema))

table = connection.table('warehouse')
for row in all_data.collect():
    table.put(bin(row['w_warehouse_sk']), {
        'str:w_warehouse_id': row['w_warehouse_id'],
        'str:w_warehouse_name': row['w_warehouse_name'],
        'int:w_warehouse_sq_ft': struct.pack('i', row['w_warehouse_sq_ft']),
        'str:w_street_number': row['w_street_number'],
        'str:w_street_name': row['w_street_name'],
        'str:w_street_type': row['w_street_type'],
        'str:w_suite_number': row['w_suite_number'],
        'str:w_city': row['w_city'],
        'str:w_county': row['w_county'],
        'str:w_state': row['w_state'],
        'str:w_zip': row['w_zip'],
        'str:w_country': row['w_country'],
        'double:w_gmt_offset': struct.pack('d', row['w_gmt_offset'])
    })

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType
sp = SparkSession.builder.appName('loader').getOrCreate()
import struct
import happybase

def ca_filter(data):
    tmp = data.split('|')
    for i in range(len(tmp)):
        if i in [0]:
            if tmp[i] == '':
                tmp[i] = 0
            else:
                tmp[i] = int(tmp[i])
        elif i in [11]:
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

ca_schema = StructType([
    StructField('ca_address_sk', LongType(), False),
    StructField('ca_address_id', StringType(), False),
    StructField('ca_street_number', StringType(), True),
    StructField('ca_street_name', StringType(), True),
    StructField('ca_street_type', StringType(), True),
    StructField('ca_suite_number', StringType(), True),
    StructField('ca_city', StringType(), True),
    StructField('ca_county', StringType(), True),
    StructField('ca_state', StringType(), True),
    StructField('ca_zip', StringType(), True),
    StructField('ca_country', StringType(), True),
    StructField('ca_gmt_offset', FloatType(), True),
    StructField('ca_location_type', StringType(), True)
])

data_path = 'hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/'
data = sp.sparkContext.textFile(data_path + 'customer_address/customer_address_01.dat')
all_data = sp.createDataFrame(data.map(ca_filter), ca_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(data_path + 'customer_address/customer_address_{:02d}.dat'.format(i))
    all_data = all_data.union(data.map(ca_filter).toDF(schema=ca_schema))
connection = happybase.Connection('localhost', 9090)
table = connection.table('customer_address')
for row in all_data.collect():
    table.put(bin(row['ca_address_sk']), {
        'str:ca_address_id': row['ca_address_id'],
        'str:ca_street_number': row['ca_street_number'],
        'str:ca_street_name': row['ca_street_name'],
        'str:ca_street_type': row['ca_street_type'],
        'str:ca_suite_number': row['ca_suite_number'],
        'str:ca_city': row['ca_city'],
        'str:ca_county  ': row['ca_county'],
        'str:ca_state': row['ca_state'],
        'str:ca_zip': row['ca_zip'],
        'str:ca_country': row['ca_country'],
        'float:ca_gmt_offset': struct.pack('f', row['ca_gmt_offset']),
        'str:ca_location_type': row['ca_location_type']
    })
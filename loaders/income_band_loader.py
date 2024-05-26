import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType
sp = SparkSession.builder.appName('loader').getOrCreate()
import struct
import happybase

connection = happybase.Connection('localhost', 9090)

def ib_filter(data):
    tmp = data.split('|')
    for i in range(len(tmp)):
        if i in [0, 1, 2]:
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

ib_schema = StructType([
    StructField('ib_income_band_sk', LongType(), False),
    StructField('ib_lower_bound', IntegerType(), True),
    StructField('ib_upper_bound', IntegerType(), True)
])

data_path = 'hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/'
data = sp.sparkContext.textFile(data_path + 'income_band/income_band_{:02d}.dat'.format(1))
all_data = sp.createDataFrame(data.map(ib_filter), ib_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(data_path + 'income_band/income_band_{:02d}.dat'.format(i))
    all_data = all_data.union(data.map(ib_filter).toDF(schema=ib_schema))
table = connection.table('income_band')
a = 0
for row in all_data.collect():
    table.put(bin(a), {
        'long:ib_income_band_sk': struct.pack('l', row['ib_income_band_sk']),
        'int:ib_lower_bound': struct.pack('i', row['ib_lower_bound']),
        'int:ib_upper_bound': struct.pack('i', row['ib_upper_bound'])
    })
    a += 1

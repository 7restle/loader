import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType
sp = SparkSession.builder.appName('loader').getOrCreate()
import struct
import happybase

def cd_filter(data):
    tmp = data.split('|')
    for i in range(len(tmp)):
        if i in [0, 4, 6, 7, 8]:
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

cd_schema = StructType([
    StructField('cd_demo_sk', LongType(), False),
    StructField('cd_gender', StringType(), True),
    StructField('cd_marital_status', StringType(), True),
    StructField('cd_education_status', StringType(), True),
    StructField('cd_purchase_estimate', IntegerType(), True),
    StructField('cd_credit_rating', StringType(), True),
    StructField('cd_dep_count', IntegerType(), True),
    StructField('cd_dep_employed_count', IntegerType(), True),
    StructField('cd_dep_college_count', IntegerType(), True)
])

data_path = 'hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/'
data = sp.sparkContext.textFile(data_path + 'customer_demographics/customer_demographics_01.dat')
all_data = sp.createDataFrame(data.map(cd_filter), cd_schema)
connection = happybase.Connection('localhost', 9090)
table = connection.table('date_dim')
a = 0
for row in all_data.collect():
    table.put(bin(a), {
        'long:cd_demo_sk': struct.pack('l', row['cd_demo_sk']),
        'str:cd_gender': row['cd_gender'],
        'str:cd_marital_status': row['cd_marital_status'],
        'str:cd_education_status': row['cd_education_status'],
        'int:cd_purchase_estimate': struct.pack('i', row['cd_purchase_estimate']),
        'str:cd_credit_rating': row['cd_credit_rating'],
        'int:cd_dep_count': struct.pack('i', row['cd_dep_count']),
        'int:cd_dep_employed_count': struct.pack('i', row['cd_dep_employed_count']),
        'int:cd_dep_college_count': struct.pack('i', row['cd_dep_college_count'])
    })
    a += 1


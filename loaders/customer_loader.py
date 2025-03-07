import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType
sp = SparkSession.builder.appName('loader').getOrCreate()
import struct
import happybase

connection = happybase.Connection('localhost', 9090)

def customer_filter(data):
    tmp = data.split('|')
    for i in range(len(tmp)):
        if i in [0, 2, 3, 4, 5, 6, 11, 12, 13]:
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

customer_schema = StructType([
    StructField('c_customer_sk', LongType(), False),
    StructField('c_customer_id', StringType(), False),
    StructField('c_current_cdemo_sk', LongType(), True),
    StructField('c_current_hdemo_sk', LongType(), True),
    StructField('c_current_addr_sk', LongType(), True),
    StructField('c_first_shipto_date_sk', LongType(), True),
    StructField('c_first_sales_date_sk', LongType(), True),
    StructField('c_salutation', StringType(), True),
    StructField('c_first_name', StringType(), True),
    StructField('c_last_name', StringType(), True),
    StructField('c_preferred_cust_flag', StringType(), True),
    StructField('c_birth_day', IntegerType(), True),
    StructField('c_birth_month', IntegerType(), True),
    StructField('c_birth_year', IntegerType(), True),
    StructField('c_birth_country', StringType(), True),
    StructField('c_login', StringType(), True),
    StructField('c_email_address', StringType(), True),
    StructField('c_last_review_date', StringType(), True)
])

data_path = 'hdfs://namenode01:9000/user/ubuntu/benchmarks/bigbench/data/'
data = sp.sparkContext.textFile(data_path + 'customer/customer_{:02d}.dat'.format(1))
all_data = sp.createDataFrame(data.map(customer_filter), customer_schema)
for i in range(2, 81):
    data = sp.sparkContext.textFile(data_path + 'customer/customer_{:02d}.dat'.format(i))
    all_data = all_data.union(data.map(customer_filter).toDF(schema=customer_schema))
table = connection.table('customer')
a = 0

for row in all_data.collect():
    table.put(bin(a), {
        'long:c_customer_sk': struct.pack('l', row['c_customer_sk']),
        'str:c_customer_id': row['c_customer_id'],
        'long:c_current_cdemo_sk': struct.pack('l', row['c_current_cdemo_sk']),
        'long:c_current_hdemo_sk': struct.pack('l', row['c_current_hdemo_sk']),
        'long:c_current_addr_sk': struct.pack('l', row['c_current_addr_sk']),
        'long:c_first_shipto_date_sk': struct.pack('l', row['c_first_shipto_date_sk']),
        'long:c_first_sales_date_sk': struct.pack('l', row['c_first_sales_date_sk']),
        'str:c_salutation': row['c_salutation'],
        'str:c_first_name': row['c_first_name'],
        'str:c_last_name': row['c_last_name'],
        'str:c_preferred_cust_flag': row['c_preferred_cust_flag'],
        'int:c_birth_day': struct.pack('i', row['c_birth_day']),
        'int:c_birth_month': struct.pack('i', row['c_birth_month']),
        'int:c_birth_year': struct.pack('i', row['c_birth_year']),
        'str:c_birth_country': row['c_birth_country'],
        'str:c_login': row['c_login'],
        'str:c_email_address': row['c_email_address'],
        'str:c_last_review_date': row['c_last_review_date']
    })
    a += 1

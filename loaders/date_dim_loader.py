import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, DoubleType
sp = SparkSession.builder.appName('loader').getOrCreate()
import struct
import happybase

def date_filter(data):
    tmp = data.split('|')
    for i in range(len(tmp)):
        if i in [0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 19, 20, 21, 22]:
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

date_schema = StructType([
    StructField('d_date_sk', LongType(), False),
    StructField('d_date_id', StringType(), False),
    StructField('d_date', StringType(), True),
    StructField('d_month_seq', IntegerType(), True),
    StructField('d_week_seq', IntegerType(), True),
    StructField('d_quarter_seq', IntegerType(), True),
    StructField('d_year', IntegerType(), True),
    StructField('d_dow', IntegerType(), True),
    StructField('d_moy', IntegerType(), True),
    StructField('d_dom', IntegerType(), True),
    StructField('d_qoy', IntegerType(), True),
    StructField('d_fy_year', IntegerType(), True),
    StructField('d_fy_quarter_seq', IntegerType(), True),
    StructField('d_fy_week_seq', IntegerType(), True),
    StructField('d_day_name', StringType(), True),
    StructField('d_quarter_name', StringType(), True),
    StructField('d_holiday', StringType(), True),
    StructField('d_weekend', StringType(), True),
    StructField('d_following_holiday', StringType(), True),
    StructField('d_first_dom', IntegerType(), True),
    StructField('d_last_dom', IntegerType(), True),
    StructField('d_same_day_ly', IntegerType(), True),
    StructField('d_same_day_lq', IntegerType(), True),
    StructField('d_current_day', StringType(), True),
    StructField('d_current_week', StringType(), True),
    StructField('d_current_month', StringType(), True),
    StructField('d_current_quarter', StringType(), True),
    StructField('d_current_year', StringType(), True)
])

data_path = 'hdfs://localhost:9000/user/bigdata/benchmarks/bigbench/data/'
data = sp.sparkContext.textFile(data_path + 'date_dim/date_dim_01.dat')
all_data = sp.createDataFrame(data.map(date_filter), date_schema)
connection = happybase.Connection('localhost', 9090)
table = connection.table('date_dim')
a = 0
for row in all_data.collect():
    table.put(bin(a), {
        'long:d_date_sk': struct.pack('l', row['d_date_sk']),
        'str:d_date_id': row['d_date_id'],
        'str:d_date': str(row['d_date']),
        'int:d_month_seq': struct.pack('i', row['d_month_seq']),
        'int:d_week_seq': struct.pack('i', row['d_week_seq']),
        'int:d_quarter_seq': struct.pack('i', row['d_quarter_seq']),
        'int:d_year': struct.pack('i', row['d_year']),
        'int:d_dow': struct.pack('i', row['d_dow']),
        'int:d_moy': struct.pack('i', row['d_moy']),
        'int:d_dom': struct.pack('i', row['d_dom']),
        'int:d_qoy': struct.pack('i', row['d_qoy']),
        'int:d_fy_year': struct.pack('i', row['d_fy_year']),
        'int:d_fy_quarter_seq': struct.pack('i', row['d_fy_quarter_seq']),
        'int:d_fy_week_seq': struct.pack('i', row['d_fy_week_seq']),
        'str:d_day_name': row['d_day_name'],
        'str:d_quarter_name': row['d_quarter_name'],
        'str:d_holiday': row['d_holiday'],
        'str:d_weekend': row['d_weekend'],
        'str:d_following_holiday': row['d_following_holiday'],
        'int:d_first_dom': struct.pack('i', row['d_first_dom']),
        'int:d_last_dom': struct.pack('i', row['d_last_dom']),
        'int:d_same_day_ly': struct.pack('i', row['d_same_day_ly']),
        'int:d_same_day_lq': struct.pack('i', row['d_same_day_lq']),
        'str:d_current_day': row['d_current_day'],
        'str:d_current_week': row['d_current_week'],
        'str:d_current_month': row['d_current_month'],
        'str:d_current_quarter': row['d_current_quarter'],
        'str:d_current_year': row['d_current_year']
    })
    a += 1

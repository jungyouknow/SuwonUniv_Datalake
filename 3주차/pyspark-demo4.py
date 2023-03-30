import pyspark

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

conf = pyspark.SparkConf()
conf.set('spark.driver.host', '127.0.0.1')
conf.set('spark.hadoop.fs.s3a.access.key', '')    #AWS Access Key
conf.set('spark.hadoop.fs.s3a.secret.key', '')   #AWS Secret Key
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2')   #Apache Hadoop 버전에 맞는 Library 설치
conf.set('spark.local.dir', 'C:/Temp')

spark = SparkSession.builder \
    .config(conf=conf) \
        .appName('S3 Bucket Reader Program') \
            .getOrCreate()

#공통칼럼
new_cols_expr = ['VendorID as vendor_id', 'passenger_count','trip_distance','RatecodeID as ratecode_id', 'store_and_fwd_flag',
                    'PULocationID as pu_location_id','DOLocationID as do_location_id','payment_type','fare_amount','extra',
                    'mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','congestion_surcharge']

#yellow_df용 칼럼
yellow_cols = ['tpep_pickup_datetime as pickup_datetime', 'tpep_dropoff_datetime as dropoff_datetime']
yellow_cols.extend(new_cols_expr)

#green_df용 칼럼
green_cols = ['lpep_pickup_datetime as pickup_datetime','lpep_dropoff_datetime as dropoff_datetime']
green_cols.extend(new_cols_expr)


input_path = 's3a://' + '' + '/input'
output_path = 's3a://' + '' + '/output'

count = 0
for ym in ['2022-01', '2022-02', '2022-03', '2022-04', '2022-05', '2022-06', '2022-07', '2022-08', '2022-09', '2022-10', '2022-11']:
    green_df = spark.read.parquet(f'{input_path}/green_tripdata_{ym}.parquet') \
        .selectExpr(green_cols).withColumn('taxi_type', f.lit('GREEN'))
    yellow_df = spark.read.parquet(f'{input_path}/yellow_tripdata_{ym}.parquet') \
        .selectExpr(yellow_cols).withColumn('taxi_type', f.lit('YELLOW'))
    union_df = green_df.union(yellow_df)
    union_df.repartition(1).write.option('header', 'true').mode('overwrite').csv(f'{output_path}/ym={ym}/')
    count += 1
print(f'{count} Files Successfully Saved')
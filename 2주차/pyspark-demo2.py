import pyspark

from pyspark.sql import SparkSession

conf = pyspark.SparkConf()
conf.set('spark.driver.host', '127.0.0.1')
conf.set('spark.hadoop.fs.s3a.access.key', '')    #AWS Access Key
conf.set('spark.hadoop.fs.s3a.secret.key', '')   #AWS Secret Key
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2')   #Apache Hadoop 버전에 맞는 Library 설치

spark = SparkSession.builder \
    .config(conf=conf) \
        .appName('S3 Bucket Reader Program') \
            .getOrCreate()

input_path = 's3a://' + '' + '/input'  #Bucket name
print('Green Taxi Data')
green_df = spark.read.parquet(f'{input_path}/green_tripdata_2022-10.parquet')
new_green_df = green_df.selectExpr('VendorID as vendor_id', 
                    'lpep_pickup_datetime as pickup_datetime',
                    'lpep_dropoff_datetime as dropoff_datetime',
                    'passenger_count',
                    'trip_distance',
                    'RatecodeID as ratecode_id',
                    'store_and_fwd_flag',
                    'PULocationID as pu_location_id',
                    'DOLocationID as do_location_id',
                    'payment_type',
                    'fare_amount',
                    'extra',
                    'mta_tax',
                    'tip_amount',
                    'tolls_amount',
                    'improvement_surcharge',
                    'total_amount',
                    'congestion_surcharge')
new_green_df.show(truncate=False)

print('------------------------------')
print('Yellow Taxi Data')
yellow_df = spark.read.parquet(f'{input_path}/yellow_tripdata_2022-10.parquet')
new_yellow_df = yellow_df.selectExpr('VendorID as vendor_id', 
                    'tpep_pickup_datetime as pickup_datetime',
                    'tpep_dropoff_datetime as dropoff_datetime',
                    'passenger_count',
                    'trip_distance',
                    'RatecodeID as ratecode_id',
                    'store_and_fwd_flag',
                    'PULocationID as pu_location_id',
                    'DOLocationID as do_location_id',
                    'payment_type',
                    'fare_amount',
                    'extra',
                    'mta_tax',
                    'tip_amount',
                    'tolls_amount',
                    'improvement_surcharge',
                    'total_amount',
                    'congestion_surcharge')
new_yellow_df.show(truncate=False)

union_df = new_green_df.union(new_yellow_df)
union_df.show(truncate=False)
"""
    이 코드 역시 에러를 발생하거나 값이 처리가 되지 않아 null로 출력될 수 있다.
    하지만, AWS Glue Studio의 Job을 생성해서 실행하기 때문에 걱정하지 말 것.
"""

import pyspark

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import boto3


ssm = boto3.client('ssm')
ssm_parameter = ssm.get_parameter(Name='', WithDecryption=True)

conf = pyspark.SparkConf()
conf.set('spark.driver.host', '127.0.0.1')
conf.set('spark.hadoop.fs.s3a.access.key', '')    #AWS Access Key
conf.set('spark.hadoop.fs.s3a.secret.key', '')   #AWS Secret Key
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2,mysql:mysql-connector-java:8.0.31')   #Apache Hadoop 버전에 맞는 Library 설치 및 JDBC Driver 설치
conf.set('spark.local.dir', 'C:/Temp')

spark = SparkSession.builder \
    .config(conf=conf) \
        .appName('Amazon Athena Program') \
            .getOrCreate()

host_ = 'jdbc:mysql://' + '' + '/newyork_taxi'
user_ = 'admin'
password_ = ssm_parameter['Parameter']['Value']
table_ = 'taxi_zone_lookup'

df = spark.read.format('jdbc').option('url', host_).option('driver', 'com.mysql.cj.jdbc.Driver') \
            .option('dbtable', table_).option('user', user_).option('password', password_) \
            .load().selectExpr('LocationID as location_id', 'borough', 'Zone as zone', 'service_zone')

# df.show()
taxi = spark.read.option('header', 'true').csv('s3a://' + '' + '/output/ym=2022-11')
# taxi.show(truncate=False)

join_df = taxi.join(df, taxi.pu_location_id == df.location_id, 'left')
# join_df.show(truncate=False)
 
# join_df.limit(20).withColumn('pickup_datetime', f.to_timestamp('pickup_datetime', "yyyy-MM-dd HH:mm:ss.SSSX")) \
#            .withColumn('dropoff_datetime', f.to_timestamp('dropoff_datetime', "yyyy-MM-dd HH:mm:ss.SSSX")) \
#             .withColumn('time_diff', f.unix_timestamp('dropoff_datetime') - f.unix_timestamp('pickup_datetime')) \
#             .show(truncate=False)

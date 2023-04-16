"""
    어떤 OS에서 실행하느냐에 따라 오류가 발생할 수 있다.
    하지만, AWS Glue Studio의 Job을 생성해서 실행하기 때문에 걱정하지 말 것.
"""
import pyspark

from pyspark.sql import SparkSession
import boto3   # pip install boto3


ssm = boto3.client('ssm')
ssm_parameter = ssm.get_parameter(Name='', WithDecryption=True)
# print(ssm_parameter)

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
# password_ = 'datalakemysql'
password_ = ssm_parameter['Parameter']['Value']
table_ = 'taxi_zone_lookup'

df = spark.read.format('jdbc').option('url', host_).option('driver', 'com.mysql.cj.jdbc.Driver') \
            .option('dbtable', table_).option('user', user_).option('password', password_).load()
            
df.show()
import pyspark

from pyspark.sql import SparkSession

conf = pyspark.SparkConf()
conf.set('spark.driver.host', '127.0.0.1')
conf.set('spark.hadoop.fs.s3a.access.key', '')    #AWS Access Key
conf.set('spark.hadoop.fs.s3a.secret.key', '')   #AWS Secret Key
# Apache Hadoop 버전에 맞는 Library 설치 및 JDBC Driver 설치
# 미 설치시 java.lang.ClassNotFoundException: com.mysql.cj.jdbc.Driver 오류발생
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2,mysql:mysql-connector-java:8.0.31')   
conf.set('spark.local.dir', 'C:/Temp')

spark = SparkSession.builder \
    .config(conf=conf) \
        .appName('Amazon RDS Program') \
            .getOrCreate()

host_ = 'jdbc:mysql://' + '' + '/newyork_taxi'
user_ = 'admin'
password_ = 'datalakemysql'
table_ = 'taxi_zone_lookup'

df = spark.read.format('jdbc').option('url', host_).option('driver', 'com.mysql.cj.jdbc.Driver') \
            .option('dbtable', table_).option('user', user_).option('password', password_).load()
            
df.show()
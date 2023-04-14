"""
이 파일을 실행하기 전에 다음과 같이 각 라이브러리 버전을 맞추어야 한다.
    1. Java 11
        -설치 후 JAVA_HOME 새로 설정
        -PATH 재 설정
    2. Spark 3.2.4
    3. Hadoop : 3.3.2
        -Spark Homepage에서 spark-3.2.4-bin-hadoop3.2.tgz 다운로드
        -SPARK_HOME 설정
        -PATH 재 설정
    4. Python 3.10.x
        -다운로드 후 설치
        -PATH 재 설정
    5. OS 환경변수에 다음 변수 추가할 것
        -PYSPARK_PYTHON=python
    6. DataLake 가상환경을 새로 생성
        -python -m venv DataLake
        -Windows
            --cd DataLake/Scripts
            --activate
        -macOS
            --$ source DataLake/Scripts/activate
    7. pip로 필요한 library 설치
        pip install pyspark==3.2.4
"""

import pyspark

from pyspark.sql import SparkSession

conf = pyspark.SparkConf()
conf.set('spark.driver.host', '127.0.0.1')
conf.set('spark.hadoop.fs.s3a.access.key', '')    #AWS Access Key
conf.set('spark.hadoop.fs.s3a.secret.key', '')   #AWS Secret Key
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2')   #Apache Hadoop 버전에 맞는 Library 설치
conf.set('spark.local.dir', 'C:/Temp')

spark = SparkSession.builder \
    .config(conf=conf) \
        .appName('Amazon Athena Program') \
            .getOrCreate()

print(f'Hadoop Version = {spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}')

data = [
    ("1101", '{"name" : "한송이", "city" : "Seoul", "gender" : "F"}', {"kor" : 78, "eng" : 87, "mat" : 83, "edp" : 78}),
    ("1102", '{"name" : "정다워", "city" : "Busan", "gender" : "M"}', {"kor" : 88, "eng" : 83, "mat" : 57, "edp" : 98}),
    ("1103", '{"name" : "그리운", "city" : "Incheon", "gender" : "M"}', {"kor" : 76, "eng" : 56, "mat" : 87, "edp" : 78}),
    ("1104", '{"name" : "고아라", "city" : "Daegu", "gender" : "F"}', {"kor" : 83, "eng" : 57, "mat" : 88, "edp" : 73}),
    ("1105", '{"name" : "사랑해", "city" : "Busan", "gender" : "F"}', {"kor" : 87, "eng" : 87, "mat" : 53, "edp" : 55}),
    ("1106", '{"name" : "튼튼이", "city" : "Gwangju", "gender" : "M"}', {"kor" : 98, "eng" : 97, "mat" : 93, "edp" : 88}),
    ("1107", '{"name" : "한아름", "city" : "Daegu", "gender" : "F"}', {"kor" : 68, "eng" : 67, "mat" : 83, "edp" : 89}),
    ("1108", '{"name" : "더크게", "city" : "Busan", "gender" : "M"}', {"kor" : 98, "eng" : 67, "mat" : 93, "edp" : 78}),
    ("1109", '{"name" : "더높이", "city" : "Seoul", "gender" : "M"}', {"kor" : 88, "eng" : 99, "mat" : 53, "edp" : 88}),
    ("1110", '{"name" : "아리랑", "city" : "Incheon", "gender" : "M"}', {"kor" : 68, "eng" : 79, "mat" : 63, "edp" : 66}),
    ("1111", '{"name" : "한산섬", "city" : "Busan", "gender" : "M"}', {"kor" : 98, "eng" : 89, "mat" : 73, "edp" : 78}),
    ("1112", '{"name" : "하나로", "city" : "Daegu", "gender" : "F"}', {"kor" : 89, "eng" : 97, "mat" : 78, "edp" : 88})
]

schema = ['hakbun', 'info', 'subjects']

df = spark.createDataFrame(data=data, schema=schema)
# df.printSchema()
# df.show(truncate=False)

output_path = 's3a://' + '' + '/output'
df.repartition(1).write.mode('overwrite').json(f'{output_path}/sample-json/')
print('JSON File Upload Successfully.')
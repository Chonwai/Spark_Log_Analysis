import sys
import os
import json
import findspark

findspark.init()

os.environ['SPARK_HOME'] = "/opt/spark"

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql import SQLContext
    from pyspark.sql import DataFrame
    from pyspark.sql import Row
    print("Successfully imported Spark Modules")
except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)

# spark = SparkSession.builder.master('local[*]').appName("PySparkShell").getOrCreate()
sc = SparkContext()
sqlContext = SQLContext(sc)

def logParse(log):
    log = log.replace(' -- ', ', ')
    log = log.replace('.rb: ', ', ')
    log = log.replace(', ghtorrent-', ', ')
    return log.split(', ', 4)

def loadRDD(filename):
    textFile = sc.textFile("../torrent-logs.txt")
    parsedRDD = textFile.map(logParse)
    return parsedRDD

rowrdd = loadRDD("torrent-logs.txt").cache()
schema = ["logging_level","timestamp","downloader_id","retrieval_stage","operation_specific"]
# ppl = rowrdd.map(lambda x: Row(event_processing=x[0], ght_data_retrieval=x[1], api_client=x[2], retriever=x[3], ghtorrent=x[4]))
DF_ppl = sqlContext.createDataFrame(data=rowrdd, schema = schema)
# rdd = sc.parallelize(DF_ppl)
# df = rdd.toDF()
# df.show()
DF_ppl.printSchema()
DF_ppl.show(100)
new_DF = DF_ppl.toDF("logging_level","timestamp","downloader_id","retrieval_stage","operation_specific")
new_DF.show(100)
new_DF.filter(new_DF.logging_level == "INFO").count()
# DF_ppl.filter(DF_ppl.logging_level == "INFO").count()

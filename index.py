import sys
import os
import json
from pyspark import SparkContext
sc = SparkContext()

os.environ['SPARK_HOME'] = "./spark-3.0.2-bin-hadoop3.2"

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

# spark = SparkSession.builder.master('local[*]')
# sc = spark.sparkContext
sc = SparkContext()

def logParse(log):
    log = log.replace(' -- ', ', ')
    log = log.replace('.rb: ', ', ')
    log = log.replace(', ghtorrent-', ', ')
    return log.split(', ', 4)

def loadRDD(filename):
    textFile = sc.textFile("./torrent-logs.txt")
    parsedRDD = textFile.map(logParse)
    return parsedRDD

rowrdd = loadRDD("torrent-logs.txt").cache()
result = rowrdd.take(10)
print(result)
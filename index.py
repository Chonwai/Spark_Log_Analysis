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
result = rowrdd.take(10000)
print(result)
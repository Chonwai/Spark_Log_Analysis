import sys
import os
import json
import findspark
import itertools

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

sc = SparkContext()
sqlContext = SQLContext(sc)

def logParse(log):
    log = log.replace(' -- ', ', ')
    log = log.replace('.rb: ', ', ')
    log = log.replace(', ghtorrent-', ', ')
    return log.split(', ', 4)

def loadRDD(filename):
    textFile = sc.textFile("../" + filename )
    parsedRDD = textFile.map(logParse)
    return parsedRDD

def loadCSVRDD(filename):
    textFile = sc.textFile("../" + filename )
    interestingRDD = textFile.map(lambda line: line.split(","))
    interestingRDD.count()
    return interestingRDD

def parseRepos(x):
    try:
        split = x[4].split('/')[4:6]
        joinedSplit = '/'.join(split)
        result = joinedSplit.split('?')[0]
    except: 
        result = ''
    x.append(result)
    return x

def changeRepo(x):
    try:
        x[5] = x[5].split("/")[1]
    except:
        x[5] = ''
    return x

rowrdd = loadRDD("torrent-logs.txt").cache()
schema = ["logging_level","timestamp","downloader_id","retrieval_stage","operation_specific"]
DF_ppl = sqlContext.createDataFrame(data=rowrdd, schema = schema)
DF_ppl.printSchema()
DF_ppl.show(100)

print("Question 1 - Count the number of messages in the category of “INFO”?")
answer1 = rowrdd.filter(lambda x : x[0]== 'INFO').count()
print(answer1)

print("Question 2 - Based on the information of retrieval stage “api_client”, count the number of processed repositories?")
filteredRdd = rowrdd.filter(lambda x: len(x) == 5) 
apiRdd = filteredRdd.filter(lambda x: x[3] == "api_client")
reposRdd = apiRdd.map(parseRepos)
removedEmpty = reposRdd.filter(lambda x: x[5] != '')
uniqueRepos = removedEmpty.groupBy(lambda x: x[5])
print(uniqueRepos.count())

print("Question 3 - Which client (downloader id) did most FAILED HTTP requests?")
onlyFailed = apiRdd.filter(lambda x: x[4].split(' ', 1)[0] == "Failed")
usersFailedHttp = onlyFailed.groupBy(lambda x: x[2])
usersFailedHttpSum = usersFailedHttp.map(lambda x: (x[0], x[1].__len__()))
print(usersFailedHttpSum.max(key=lambda x: x[1]))

print("Question 4 - What is the top-5 active repository (based on messages from ghtorrent.rb)?")
activityRepos = removedEmpty.groupBy(lambda x: x[5])
countActivityRepos = activityRepos.map(lambda x: (x[0], x[1].__len__()))
print(countActivityRepos.top(5, key=lambda x: x[1]))

print("Question 5 - How many records in the log file refer to the records in the interesting repositories?")
interestingRDD = loadCSVRDD("interesting-repos.csv").cache()
interestingRepo = interestingRDD.keyBy(lambda x: x[3])
logLineRepo = reposRdd.map(changeRepo).filter(lambda x: x[5] != '').keyBy(lambda x: x[5])
joinedRepo = interestingRepo.join(logLineRepo)
print(joinedRepo.count())

print("Question 6 - Which of the interesting repositories has the most failed API calls?")
# repositories = joinedRepo.filter(lambda (key,(k,v)): v[4].startswith("Failed")).map(lambda (key, (k, v)): (key, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda (k, v): v, False)
# print(joinedRepo.filter(lambda (key, [k,v]): v[4].startswith("Failed")).collect())
print(joinedRepo.keys().collect())
# print(joinedRepo.collect())
# print(repositories)
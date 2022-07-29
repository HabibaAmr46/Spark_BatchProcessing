!apt-get install openjdk-8-jdk-headless -qq > /dev/null

# install spark (change the version number if needed)
!wget -q https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz

# unzip the spark file to the current folder
!tar xf spark-3.0.0-bin-hadoop3.2.tgz

# set your spark folder to your system path environment. 
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.0.0-bin-hadoop3.2"


# install findspark using pip
!pip install -q findspark
!pip install pyspark

import findspark
findspark.init()
 
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate() 
spark

#task 1

from pyspark import SparkConf, SparkContext
sc = SparkContext.getOrCreate();
def parseLine(line):
 fields = line.split(' ')
 code = fields[0]
 title= fields[1]
 hits = fields[2]
 size = fields[3]
 return (code, title, hits,size)
lines = sc.textFile("a.out")
parsedLines = lines.map(parseLine)
res=parsedLines.take(10)
for i in range (10):
  print(res[i]);

#task 2

from pyspark import SparkConf, SparkContext
sc = SparkContext.getOrCreate();
def parseLine(line):
 fields = line.split(' ')
 size = int(fields[3])
 return (size)
lines = sc.textFile("a.out")
parsedLines = lines.map(parseLine)
minvalue=parsedLines.min()
print(minvalue)
maxvalue=parsedLines.max()
print(maxvalue)
counts=parsedLines.count()
sums=parsedLines.sum()
print(sums/counts)
#task3
from pyspark import SparkConf, SparkContext
sc = SparkContext.getOrCreate();
def parseLine(line):
 fields = line.split(' ')
 code=fields[0]
 title = fields[1]
 return (code,title)
lines = sc.textFile("a.out")
parsedLines = lines.map(parseLine).filter(lambda x:x[1].startswith("The"))
title=parsedLines.count()
print(title)
filtered_en=parsedLines.filter(lambda x:x[0]!='en')
filtered_en.count()
#task 4

sc = SparkContext.getOrCreate();
def parseLine(line):
 fields = line.split(' ')
 title= fields[1]
 return (title)

def lower_clean_str(x):
  punc='!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
  lowercased_str = x.lower()
  for ch in punc:
    lowercased_str = lowercased_str.replace(ch, '')
  return lowercased_str

lines = sc.textFile("pagecounts-20160101-000000_parsed.out")
parsedLines = lines.map(parseLine).flatMap(lambda line: line.split("_"))
rdd=parsedLines.map(lower_clean_str)
result = rdd.map(lambda title : (title, 1)).reduceByKey(lambda x,y: x + y).filter(lambda x: x[1]==1)
result.collect()

#task 5

sc = SparkContext.getOrCreate();
def parseLine(line):
 fields = line.split(' ')
 title = fields[1]
 return (title)
lines = sc.textFile("a.out")
parsedLines = lines.map(parseLine).map(lambda title : (title, 1)).reduceByKey(lambda x,y: x + y)
final=parsedLines.max(lambda x:x[1])
print(final)
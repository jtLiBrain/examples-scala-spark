# coding=utf-8

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext, Column, Row
import pyspark.sql.functions as F
from pyspark.sql.types import *
import random
import json
import sys

# 1. https://www.cnblogs.com/wdmx/p/10156500.html


reload(sys)
sys.setdefaultencoding("utf8")

spark = SparkSession \
    .builder \
    .appName("test") \
    .master("local[*]")\
    .getOrCreate()

testCollectDF = spark.read.format("mongo") \
        .option("uri", "mongodb://xxxx:xxxxx@xxx.xxx.xxx.xxx:27022/assess?readPreference=primaryPreferred") \
        .option("collection", "testCollect") \
        .load()

testCollectDF.show(10, False)


spark.stop




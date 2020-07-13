from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, HiveContext, Column, Row
import pyspark.sql.functions as F
from pyspark.sql.types import *
import random

# 1. https://www.cnblogs.com/wdmx/p/10156500.html


spark = SparkSession \
    .builder \
    .appName("test") \
    .master("local[*]")\
    .getOrCreate()

spark.conf.set("spark.sql.execution.arrow.enabled", "true")



def example1():
    """
    Success
    :return:
    """

    df = spark.createDataFrame([(1, 1), (2, 2), (1, 2), (1, 2)], ["key", "value"])
    resultDF = df.groupBy("key").agg(F.sum(F.col("value")))
    resultDF.show()


def example2():
    """
    Success
    :return:
    """

    def testFun(v):
        return v

    udf_random_col = F.udf(testFun, IntegerType())

    df = spark.createDataFrame([(1, 1), (2, 2), (1, 2), (1, 2)], ["key", "value"])
    resultDF2 = df.groupBy(F.col("key")).agg(F.sum(udf_random_col(F.col("value"))))
    resultDF2.show()


def example4():
    """
    Success
    :return:
    """

    def testFun(v):
        return v

    udf_random_col = F.udf(testFun, IntegerType())


    df = spark.createDataFrame([("1", "a", 1), ("1", "a", 2)], ["id", "name", "value"])
    df.rdd.map(lambda s: ((s[0], s[1]), s[2]))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda s: (s[0][0],s[0][1],s[1]))\
        .toDF()\
        .show(5)

def example3():
    """
    Success
    :return:
    """
    df = spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])

    my_filter = F.udf(lambda a: a == 1, BooleanType())
    resultDF1 = df.select(F.col("key")).distinct().filter(my_filter(F.col("key")))
    print "Key is 1:"
    resultDF1.show()

    my_copy = F.udf(lambda x: x, IntegerType())
    my_strlen = F.udf(lambda x: len(x), IntegerType())
    my_add = F.udf(lambda a, b: int(a + b), IntegerType())

    resultDF2 = df.groupBy(my_copy(F.col("key")).alias("k")) \
        .agg(F.sum(my_strlen(F.col("value"))).alias("s")) \
        .select(my_add(F.col("k"), F.col("s")).alias("t"))

    resultDF2.show()


example4()


spark.stop




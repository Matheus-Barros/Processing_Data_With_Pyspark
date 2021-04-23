'''
@Author: Matheus Barros
Date: 23/04/2021

'''

from pyspark.context import SparkContext, SparkConf
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession

#PARALLELIZING WITH 2 CORES
conf = SparkConf().setAppName("rdd basic").setMaster("local[2]")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

fileRDD = sc.textFile('file.txt')

# Check the number of partitions in fileRDD
print("Number of partitions in fileRDD is", fileRDD.getNumPartitions())

# Create a fileRDD_part from file_path with 5 partitions
fileRDD_part = sc.textFile('file.txt', minPartitions = 5)

# Check the number of partitions in fileRDD_part
print("Number of partitions in fileRDD_part is", fileRDD_part.getNumPartitions())


file = fileRDD.collect()
print(file)
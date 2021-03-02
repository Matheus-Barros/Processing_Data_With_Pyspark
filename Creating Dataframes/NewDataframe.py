'''
@Author: Matheus Barros
Date: 03/02/2021

'''
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

purchase = Row("price","quantity","product")

record = (
        	purchase(12, 1,"cake"),
        	purchase(25, 63,"bread"),
        	purchase(78, 75,"butter")
         )

df = spark.createDataFrame((record))

df.show()
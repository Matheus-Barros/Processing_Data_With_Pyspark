'''
@Author: Matheus Barros
Date: 02/22/2021

'''
from pyspark.sql import SparkSession


'''
Creating multiple SparkSessions and SparkContexts can cause issues, 
so it's best practice to use the SparkSession.builder.getOrCreate()

'''
my_spark = SparkSession.builder.getOrCreate()

# PRINTW SESSIONS
print(my_spark)
print('\n\n')


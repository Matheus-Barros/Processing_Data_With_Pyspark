'''
@Author: Matheus Barros
Date: 28/04/2021

'''
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#CREATING A SESSION OF PYSPARK
spark = SparkSession.builder.getOrCreate()

tv_shows = spark.read.csv("netflix_titles.csv",header=True)

tv_shows.write.format('parquet').save('netflix_titles.parquet')

flight_df = spark.read.parquet('netflix_titles.parquet')

#KINDA CREATE A TABLE SQL
flight_df.createOrReplaceTempView('movie')

#QUERING A PARQUET FILE AS SQL
short_flights_df = spark.sql('SELECT * FROM movie WHERE release_year < 2000')

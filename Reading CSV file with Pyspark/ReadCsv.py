'''
@Author: Matheus Barros
Date: 03/01/2021

'''
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#CREATING A SESSION OF PYSPARK
spark = SparkSession.builder.getOrCreate()

#CREATING A SCHEMA OF THE COLUMNS TYPES
schema = StructType([StructField("show_id", StringType(), nullable = False),
					StructField("type", StringType(), nullable = False),
					StructField("title", StringType(), nullable = False),
					StructField("director", StringType(), nullable = False),
					StructField("cast", StringType(), nullable = False),
					StructField("country", StringType(), nullable = False),
					StructField("date_added", StringType(), nullable = False),
					StructField("release_year", IntegerType(), nullable = False),
					StructField("rating", StringType(), nullable = False),
					StructField("duration", StringType(), nullable = False),
					StructField("listed_in", StringType(), nullable = False),
					StructField("description", StringType(), nullable = False)])

#CREATING A DATAFRAME
tv_shows = spark.read.options(header="true").schema(schema).csv("netflix_titles.csv")

#SHOWING DATAFRAME
tv_shows.show()

#SHOWING DATA TYPES
print(tv_shows.dtypes)
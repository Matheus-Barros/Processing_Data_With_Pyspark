'''
@Author: Matheus Barros
Date: 03/01/2021

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col , when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType , DateType , ByteType
from datetime import date, timedelta

#CREATING A SESSION OF PYSPARK
spark = SparkSession.builder.getOrCreate()

#CREATING A SCHEMA OF THE COLUMNS TYPES
schema = StructType([StructField("Name", StringType(), nullable = False),
					StructField("Birthday", DateType(), nullable = False),
					StructField("Age", ByteType(), nullable = False)])

#CREATING A DATAFRAME
original = spark.read.options(header="true").schema(schema).csv("dataset.csv")

#FILL NULL VALUES WITH 56
original = original.fillna(56, subset=["Age"])
print('Original DataFrame')
original.show()

#DROP INVALID ROWS
data = spark.read.options(header="true",mode="DROPMALFORMED").schema(schema).csv("dataset.csv")

#SHOWING DATAFRAME
print('\nDataFrame with dropped rows')
data.show()

#GET THE DATE FROM NOW PLUS 1 YEAR
one_year_from_now = date.today().replace(year=date.today().year + 1)


#CLEAN THE DATE WHERE THE YEAR IS INCORRECT
better_frame = data.withColumn("Birthday",when(col("Birthday") > one_year_from_now, None).otherwise(col("Birthday")))

#============================================CLEANING NULL VALUES============================================

"""

 			REMOVING NULL VALUES BY COLUMN 		
	better_frame = better_frame.na.drop(subset=["Birthday"])

						OR

 			REMOVING ALL NULL VALUES 		
	better_frame = better_frame.dropna()

"""

better_frame = better_frame.dropna()



#SHOWING DATAFRAME
print('Cleaned DataFrame')
better_frame.show()
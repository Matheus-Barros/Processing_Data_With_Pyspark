'''
@Author: Matheus Barros
Date: 03/01/2021

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col , when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType , DateType , ByteType , FloatType
from datetime import date, timedelta
from pyspark.sql import functions as F

#CREATING A SESSION OF PYSPARK
spark = SparkSession.builder.getOrCreate()
						
#CREATING A SCHEMA OF THE COLUMNS TYPES
schema = StructType([StructField("id", IntegerType(), 	nullable = False),
					StructField("country", StringType(), nullable = False),
					StructField("description", StringType(), nullable = False),
					StructField("designation", StringType(), nullable = False),
					StructField("points", ByteType(), nullable = False),
					StructField("price", FloatType(), nullable = False),
					StructField("province", StringType(), nullable = False),
					StructField("region_1", StringType(), nullable = False),
					StructField("region_2", StringType(), nullable = False),
					StructField("taster_name", StringType(), nullable = False),
					StructField("taster_twitter_handle", StringType(), nullable = False),
					StructField("title", StringType(), nullable = False),
					StructField("variety", StringType(), nullable = False),
					StructField("winery", StringType(), nullable = False)])

wines_list = spark.read.options(header="true").schema(schema).csv("winemag-data-130k-v2.csv")

#FILTER WINES FROM GERMANY
wines_in_germany = wines_list.filter(col("country") == 'Germany')

#SORT VALUES DESCENDING BY PRICE
wines_in_germany = wines_in_germany.orderBy(col("points").desc())

#SELECTING COLUMNS
wines_in_germany = wines_in_germany.select(
											[col("country"),
											col("winery"),
											col("designation"),
											col("points"),
											col("price"),
											col("province").alias("province_name")])

#IT'LL GROUP BY WINERY THAT HAS GREATER AVERAGE OF POINTS. IT'LL ALSO SHOW HOW MANY RATINGS EACH WINERY HAS
wines_in_germany = (wines_in_germany
									.groupBy(col('winery'))
									.agg(
											F.mean('points').alias('Average_Price'),
											F.count('winery').alias('Quantity_of_ratings')
										)
									).orderBy(col("Average_Price").desc())

#PRINTS FRAME
wines_in_germany.show()
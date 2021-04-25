'''
@Author: Matheus Barros
Date: 25/04/2021

'''

from pyspark.context import SparkContext, SparkConf
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession

#PARALLELIZING WITH 2 CORES
conf = SparkConf().setAppName("rdd basic").setMaster("local[2]")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

iphones_RDD = sc.parallelize([
							("XS", 2018, 5.65, 2.79, 6.24),
							("XR", 2018, 5.94, 2.98, 6.84),
							("X10", 2017, 5.65, 2.79, 6.13),
							("8Plus", 2017, 6.23, 3.07, 7.12),
							("8Plus", 2017, 6.23, 3.07, 7.12)])

names = ['Model','Year','Height','Width','Weight']

iphones_df = spark.createDataFrame(iphones_RDD, schema=names)

#select() transformation subsets the columns in the DataFrame 
df_Model = iphones_df.select('Model')
df_Model.show()

#filter() transformation filters out the rows based on a condition
df_Year = iphones_df.filter(iphones_df.Year > 2017)
df_Year.show()

#groupby() operation can be used to group a variable
df_Year_Group = iphones_df.groupby('Year')
df_Year_Group.count().show()

#orderby() operation sorts the DataFrame based one or more columns
iphones_df.orderBy('Year').show()

#dropDuplicates() removes the duplicate rows of a DataFrame
test_df_no_dup = iphones_df.dropDuplicates()
test_df_no_dup.show()
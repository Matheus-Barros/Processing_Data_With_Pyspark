'''
@Author: Matheus Barros
Date: 26/04/2021

'''
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark_dist_explore import hist
import pandas as pd
import matplotlib.pyplot as plt

#CREATING A SESSION OF PYSPARK
spark = SparkSession.builder.getOrCreate()

#CREATING A DATAFRAME
tv_shows = spark.read.options(header="true").csv("netflix_titles.csv")

#CRETING PANDAS DATAFRAME WITH SPARK FRAME
test_df_sample_pandas = tv_shows.toPandas()

#CLEANING DATAFRAME
test_df_sample_pandas.query('release_year != ""',inplace=True)
test_df_sample_pandas.reset_index(drop=True,inplace=True)


test_df_sample_pandas = test_df_sample_pandas.replace(to_replace='None', value=np.nan).dropna()

test_df_sample_pandas['release_year_is_num'] = list(map(lambda x: x.isdigit(), test_df_sample_pandas['release_year']))

test_df_sample_pandas.query('release_year_is_num == True',inplace=True)

test_df_sample_pandas.reset_index(drop=True,inplace=True)

test_df_sample_pandas["release_year"] = test_df_sample_pandas["release_year"].astype(int)

#PLOTING GRAPHIC
test_df_sample_pandas.hist('release_year')
plt.show()


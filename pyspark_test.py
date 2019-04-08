# -*- coding: utf-8 -*-
"""
Created on Sat Apr  6 19:36:08 2019

@author: reza
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
import numpy as np

# Create sparksession
spark = SparkSession \
    .builder \
    .appName("LocalSparkSession") \
    .master("local[4]") \
    .getOrCreate()

# Load CSVs
# test_df_all = spark.read.format("csv").option("header", "True").load("*.csv")
main_df = spark.read.option("header", "True").csv("*.csv")

# Drop unnecessary columns
main_df = main_df.drop("idtype", "horizontalaccuracy", "placeid", "placename", 
             "placeaddress", "brandname", "categoryname", "confidence", "year",
             "month", "day", "country", "tier", "dwell_time")

# Change datatype of columns for memory optimization
# conversion_func = udf(lambda x: np.float32(x))
# main_df = main_df.withColumn("lat", "long", conversion_func(col("latitude". "longitude"))).drop("latitude", "longitude")

# Perform SQL query
main_df.registerTempTable("sql_table")
test_df = spark.sql("select \
                        idfa, \
                        cast(latitude as decimal(8, 4)) as latitude, \
                        cast(longitude as decimal(8, 4)) as longitude, \
                        cast(cast(timestamp as int) as timestamp) as timestamp \
                    from \
                        sql_table \
                    where \
                        timestamp >= 1546300800 and timestamp < 1546905599 \
                    order by \
                        timestamp")
# print(test_df.count())

#spark.conf.set("spark.sql.execution.arrow.enabled", "true")
#
#pd_df = test_df.select("*").toPandas()
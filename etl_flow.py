# -*- coding: utf-8 -*-
"""
Created on Sat Apr  6 19:36:08 2019

@author: reza
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.functions import udf
import numpy as np
import pandas as pd

# Create sparksession
spark = SparkSession \
    .builder \
    .appName("LocalSparkSession") \
    .master("local[4]") \
    .getOrCreate()

# Load CSVs
# parentDf = spark.read.format("csv").option("header", "True").load("*.csv")
filePath = "jan_dataset/*.csv"
parentDf = spark.read.option("header", "True").csv(filePath)

# Drop unnecessary columns
parentDf = parentDf.drop("idtype", "horizontalaccuracy", "placeid", "placename", 
             "placeaddress", "brandname", "categoryname", "confidence", "year",
             "month", "day", "country", "tier", "dwell_time")

# Show columns and schema
# parentDf.dtypes
parentDf.printSchema()

# Perform SQL query
parentDf.registerTempTable("parentTable")

# Window function SQL
sqlQuery =  """
            SELECT
                COUNT(adId) AS count_per_hr,
                datetime as datetime
            FROM (SELECT
                    adId AS adId,
                    datetime AS datetime,
                    COUNT(adId) OVER (PARTITION BY datetime ORDER BY datetime) AS total_count_hr
                FROM (SELECT
                        idfa as adId,
                        CAST(latitude as decimal(8, 4)) AS latitude,
                        CAST(longitude as decimal(8, 4)) AS longitude,
                        from_unixtime(cast(timestamp as int), 'yyyy-MM-dd') AS datetime
                    FROM
                        parentTable
                    WHERE
                        timestamp BETWEEN CAST(to_unix_timestamp(CAST('2019-01-31 00:00:00' AS timestamp)) AS INT)
                                    AND CAST(to_unix_timestamp(CAST('2019-01-31 19:59:59' AS timestamp)) AS INT)))
            GROUP BY datetime
            ORDER BY datetime
            """
sampleDf = spark.sql(sqlQuery)
sampleDf.show()

# Aggregate count
sqlQuery =  """
            SELECT
                COUNT(idfa) as total_count_hr,
                from_unixtime(cast(timestamp as int), 'yyyy-MM-dd HH') AS datetime
            FROM
                parentTable
            WHERE
                timestamp BETWEEN CAST(to_unix_timestamp(CAST('2019-01-01 00:00:00' AS timestamp)) AS INT)
                            AND CAST(to_unix_timestamp(CAST('2019-01-03 23:59:59' AS timestamp)) AS INT)
            GROUP BY
                datetime
            ORDER BY
                datetime
            """


#### FOLLOWING SECTION NEEDS TO BE UPDATED
# Define window function
window = Window.partitionBy('datetime').orderBy('datetime')

# Apply window function
sampleDf = sampleDf.withColumn("dailyTotal", func.count("adId").over(window))
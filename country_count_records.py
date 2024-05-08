# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import *
df = spark.read.csv("/FileStore/tables/Order.csv", header=True, inferSchema=True, sep=',')
df_select = df.select("region", "country") \
                .groupBy("region") \
                .agg(count("*").alias("count")) \
                .filter(col("region") != 'Asia') \
                .where(col("count") > 1) \
                 .withColumn("Curr_date", lit(datetime.now().strftime('%Y-%m-%d-%H:%M:%S')))
display(df_select)

df_select.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("country_count")

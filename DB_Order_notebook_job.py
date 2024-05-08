# Databricks notebook source

df = spark.read.csv("/FileStore/tables/Order.csv", header=True, inferSchema=True, sep=',')
df.printSchema()
df.show()

# COMMAND ----------

df1 = spark.read.json("/FileStore/tables/Order.json")
df1.printSchema()
df1.show()

# COMMAND ----------

df1 = spark.read.parquet("/FileStore/tables/Order.parquet")
df1.printSchema()
df1.show()

# COMMAND ----------

df_select=df.select("country","region").filter(column("Region") == 'Asia')
display(df_select)

# COMMAND ----------

from pyspark.sql.functions import *
df_select=df.select("country","region")
display(df_select)

# COMMAND ----------

#from pyspark import *.*
df_temp1 = df.filter(column("Region") == 'Asia')
df_select=df.filter(df.Region == "Asia")
#filter(col("state") == "OH")
display(df.filter("Region == 'Asia'").show())
display(df_temp1)

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.functions import column
df_select=df.select("region","country") \
			.groupBy(col("Region")).count() \
			.filter(column("Region") != 'Asia') \
			.where(column("count") > 1) \
			.withColumn(column("Curr_date"), datetime.now().strftime('%Y-%m-%d'))
display(df_select)
df_select.write.mode("append").saveAsTable("country_count")

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *
df_select = df.select("region", "country") \
                .groupBy("region") \
                .agg(count("*").alias("count")) \
                .filter(col("region") != 'Asia') \
                .where(col("count") > 1) \
                 .withColumn("Curr_date", lit(datetime.now().strftime('%Y-%m-%d-%M:%S')))
display(df_select)

df_select.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("country_count")

# COMMAND ----------

from pyspark.sql.functions import *
#display(df)
df_self=df.alias("df_self")
df_join=df.join(df_self, df.OrderID == df_self.OrderID)
display(df_join)

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_date

display(df.withColumn("Curr_date", lit(current_date())))

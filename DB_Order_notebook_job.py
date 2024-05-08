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

df_select=df.select("region","country").groupBy(column("Region")).count().filter(column("Region") != 'Asia').where(column("count") > 100)
display(df_select)

# COMMAND ----------

from pyspark.sql.functions import *
#display(df)
df_self=df.alias("df_self")
df_join=df.join(df_self, df.OrderID == df_self.OrderID)
display(df_join)

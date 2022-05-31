# Databricks notebook source
import pandas as pd
rawdata = spark.read.csv("/mnt/groupe1/Characters.csv", sep=";", header=True)
#df = rawdata.toPandas()
#Ouvre le avec spark et fais un toPandas()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS groupe1;

# COMMAND ----------

#df = df[df["House"].isin(['Gryffindor', 'Ravenclaw', 'Slytherin', 'Hufflepuff'])]
#df = df[["House", "Gender", "Blood status", "Species", "Patronus", "Death"]]
#from pyspark.sql.functions import col
filtered_data = rawdata.filter(col("House").isin(['Gryffindor', 'Ravenclaw', 'Slytherin', 'Hufflepuff']))
#data = filtered_data.select("House", "Gender", "Blood status", "Species", "Patronus", "Death").withColumnRenamed("Blood status","BloodStatus")
#data = filtered_data.select("House", "Gender", "Wand", "Blood status", "Species", "Patronus", "Loyalty", "Skills", "Birth").withColumnRenamed("Blood status", "BloodStatus")
data = filtered_data.select("Gender", "Wand", "Patronus", "Species", "Blood status", "Loyalty", "Skills", "Birth", "House").withColumnRenamed("Blood status", "BloodStatus")

# COMMAND ----------

df = data.toPandas()

# COMMAND ----------

df["EyeColour"].unique()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
mySchema = StructType([ StructField("First Name", StringType(), True)\
                       ,StructField("Age", IntegerType(), True)])

#data = spark.createDataFrame(df,schema=mySchema)

# COMMAND ----------

data.write.format("delta").mode("overwrite").option("userMetada","init").saveAsTable("groupe1.dataprep")

# COMMAND ----------

data

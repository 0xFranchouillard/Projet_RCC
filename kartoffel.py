# Databricks notebook source
import pandas as pd
rawdata = spark.read.option("encoding", "UTF-8").option("inferSchema", "true").option("multiLine", "true").csv("/mnt/groupe1/Characters.csv", sep=";", header=True)
rawdata.count()

# COMMAND ----------

rawdata.printSchema()

# COMMAND ----------

# rawdata.select('Gender').distinct().show(100, -1)
# rawdata.select('House').distinct().show(100, -1)
# rawdata.select('Species').distinct().show(100, -1)
# rawdata.select('Blood status').distinct().show(100, -1)
rawdata.select('Hair colour').distinct().show(100, -1)
# rawdata.select('Eye colour').distinct().show(100, -1)
# rawdata.select('Loyalty').distinct().show(100, -1)
# rawdata.select('Birth').distinct().show(100, -1)
# rawdata.select('Death').distinct().show(100, -1)

# COMMAND ----------

# data = rawdata.withColumn('House', (F.when(
#     (F.col('House') == "Durmstrang Institute") | 
#     (F.col('House') == "Beauxbatons Academy of Magic") |
#     (F.col('House').isNull()), "No House")
#                                     .otherwise(F.col('House'))))
from pyspark.sql import functions as F

data = rawdata.filter((F.col('House') == "Gryffindor") | (F.col('House') == "Ravenclaw") | (F.col('House') == "Slytherin") | (F.col('House') == "Hufflepuff"))
data.select('House').distinct().show()
data = data.select('House', 'Gender', 'Species', 'Blood status', 'Hair colour', 'Eye colour', 'Loyalty', 'Birth')

# COMMAND ----------

data_species = data.select('Species') \
.withColumn('Human', (F.when(F.col('Species') == "Human", F.lit(1)).otherwise(F.lit(0)))) \
.withColumn('Other', (F.when(((F.col('Species') != "Ghost") & (F.col('Species') != "Human")), F.lit(1)).otherwise(F.lit(0))))
data_species.show(100, -1)

# COMMAND ----------

data_blood = data.select('Blood status') \
.withColumn("Blood status clean", F.regexp_replace('Blood status', u'\xa0', ' ')) \
.withColumn("Pure-blood or Half-blood", (F.when(((F.lower(F.col('Blood status clean')) == "pure-blood or half-blood") | 
                                                 (F.lower(F.col('Blood status clean')) == "half-blood or pure-blood")), F.lit(1)) \
                                         .otherwise(F.lit(0)))) \
.withColumn("Pure-blood", (F.when(F.col('Blood status clean') == "Pure-blood", F.lit(1)).otherwise(F.lit(0)))) \
.withColumn("Half-blood", (F.when(F.col('Blood status clean') == "Half-blood", F.lit(1)).otherwise(F.lit(0)))) \
.withColumn("Muggle-born", (F.when(F.col('Blood status clean') == "Muggle-born", F.lit(1)).otherwise(F.lit(0)))) \
.withColumn("Other", (F.when(((F.col('Pure-blood') == 0) &
                             (F.col('Half-blood') == 0) & 
                             (F.col('Muggle-born') == 0) &
                             (F.col('Pure-blood or Half-blood') == 0)), F.lit(1)) \
                      .otherwise(F.lit(0))))
data_blood.show(100, -1)

# COMMAND ----------

display(data.select('Hair colour').groupBy('Hair Colour').count())
# data.select('Eye colour').show(100, -1)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS groupe1;
# MAGIC 
# MAGIC data_species.write.format("delta").mode("overwrite").option("userMetada","init").saveAsTable("groupe1.ft_species")
# MAGIC data_blood.write.format("delta").mode("overwrite").option("userMetada","init").saveAsTable("groupe1.ft_blood_status")

# COMMAND ----------

data_blood.write.format("delta").mode("overwrite").option("userMetada","init").saveAsTable("groupe1.ft_species")

# COMMAND ----------

data_blood

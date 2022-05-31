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
# rawdata.select('Hair colour').distinct().show(100, -1)
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
display(data_species)

# COMMAND ----------

data_blood = data.select('Blood status') \
.withColumn('Blood_status', F.col('Blood status')) \
.withColumn("Blood status", F.regexp_replace('Blood status', u'\xa0', ' ')) \
.withColumn("Pure-blood_or_Half-blood", (F.when(((F.lower(F.col('Blood status')) == "pure-blood or half-blood") | 
                                                 (F.lower(F.col('Blood status')) == "half-blood or pure-blood")), F.lit(1)) \
                                         .otherwise(F.lit(0)))) \
.withColumn("Pure-blood", (F.when(F.col('Blood status') == "Pure-blood", F.lit(1)).otherwise(F.lit(0)))) \
.withColumn("Half-blood", (F.when(F.col('Blood status') == "Half-blood", F.lit(1)).otherwise(F.lit(0)))) \
.withColumn("Muggle-born", (F.when(F.col('Blood status') == "Muggle-born", F.lit(1)).otherwise(F.lit(0)))) \
.withColumn("Other", (F.when(((F.col('Pure-blood') == 0) &
                             (F.col('Half-blood') == 0) & 
                             (F.col('Muggle-born') == 0) &
                             (F.col('Pure-blood_or_Half-blood') == 0)), F.lit(1)) \
                      .otherwise(F.lit(0)))) \
.drop('Blood status')
display(data_blood)

# COMMAND ----------

# display(data.select('Hair colour').groupBy('Hair colour').count())
data_hair = data.select('Hair colour').withColumn('Hair_colour', F.col('Hair colour')) \
.withColumn('Blond', (F.when(((F.col('Hair colour') == "Blond") |
                             (F.col('Hair colour') == "Blonde")), F.lit(1)) \
                      .otherwise(F.lit(0)))) \
.withColumn('Brown', (F.when((F.col('Hair colour') == "Brown"), F.lit(1)).otherwise(F.lit(0)))) \
.withColumn('Black', (F.when(((F.col('Hair colour') == "Dark") |
                             (F.col('Hair colour') == 'Black')), F.lit(1))
                      .otherwise(F.lit(0)))) \
.withColumn('Red', (F.when((F.col('Hair colour') == "Red"), F.lit(1)).otherwise(F.lit(0)))) \
.drop('Hair colour')
display(data_hair)

# COMMAND ----------

# display(data.select('Eye colour').groupBy('Eye colour').count())
data_eye = data.select('Eye colour').withColumn('Eye_colour', F.col('Eye colour')) \
.withColumn('Eye colour', F.regexp_replace('Eye colour', u'\xa0', '')) \
.withColumn('Green', (F.when(((F.col('Eye colour') == "Green") |
                             (F.col('Eye colour') == "Bright green") |
                             (F.col('Eye colour') == "Gooseberry")), F.lit(1)) \
                      .otherwise(F.lit(0)))) \
.withColumn('Blue', (F.when((F.col('Eye colour') == "Blue"), F.lit(1)).otherwise(F.lit(0)))) \
.withColumn('Black', (F.when(((F.col('Eye colour') == "Dark") |
                            (F.col('Eye colour') == "Black")), F.lit(1)) \
                     .otherwise(F.lit(0)))) \
.withColumn('Brown', (F.when(((F.col('Eye colour') == "Brown") |
                             (F.col('Eye colour') == "Bright brown")), F.lit(1)) \
                      .otherwise(F.lit(0)))) \
.withColumn('Grey', (F.when(((F.col('Eye colour') == "Grey") |
                            (F.col('Eye colour') == "Silvery")), F.lit(1)) \
                     .otherwise(F.lit(0)))) \
.withColumn('Scarlet', (F.when((F.col('Eye colour') == "Scarlet"), F.lit(1)).otherwise(F.lit(0)))) \
.withColumn('Hazel', (F.when((F.col('Eye colour') == "Hazel"), F.lit(1)).otherwise(F.lit(0)))) \
.drop('Eye colour')
display(data_eye)

# COMMAND ----------

# display(data.select('Loyalty').groupBy('Loyalty').count())
data_loyalty = data.select('Loyalty').withColumn('Lord_Voldemort', (F.when((F.col('Loyalty').contains("Lord Voldemort")), F.lit(1)).otherwise(F.lit(0)))) \
.withColumn('Albus_Dumbledore', (F.when((F.col('Loyalty').contains("Albus Dumbledore")), F.lit(1)).otherwise(F.lit(0)))) \
.withColumn('Ministry_of_Magic', (F.when((F.col('Loyalty').contains("Ministry of Magic")), F.lit(1)).otherwise(F.lit(0)))) \
.withColumn('Order_of_the_Phoenix', (F.when((F.col('Loyalty').contains("Order of the Phoenix")), F.lit(1)).otherwise(F.lit(0)))) \
.withColumn('Dumbledore_s_Army', (F.when((F.col('Loyalty').contains("Dumbledore's Army")), F.lit(1)).otherwise(F.lit(0)))) \
.withColumn('Hogwarts_School_of_Witchcraft_and_Wizardry', (F.when((F.col('Loyalty').contains("Hogwarts School of Witchcraft and Wizardry")), F.lit(1)).otherwise(F.lit(0)))) \
.withColumn('Death_Eaters', (F.when((F.col('Loyalty').contains("Death Eaters")), F.lit(1)).otherwise(F.lit(0))))
display(data_loyalty)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS groupe1;

# COMMAND ----------

data_species.write.format("delta").mode("overwrite").option("userMetada","init").saveAsTable("groupe1.ft_species")
data_blood.write.format("delta").mode("overwrite").option("userMetada","init").saveAsTable("groupe1.ft_blood_status")
data_hair.write.format("delta").mode("overwrite").option("userMetada","init").saveAsTable("groupe1.ft_hair_colour")
data_eye.write.format("delta").mode("overwrite").option("userMetada","init").saveAsTable("groupe1.ft_eye_colour")
data_loyalty.write.format("delta").mode("overwrite").option("userMetada","init").saveAsTable("groupe1.ft_loyalty")

# COMMAND ----------

#in progress by 0xFranchouillard
from databricks.feature_store import feature_table
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

fs.create_table(
    name="groupe1.ft_species_1",
    primary_keys="Species",
    df=data_species.distinct())

# COMMAND ----------



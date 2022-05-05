# Databricks notebook source
rawdata = spark.read.csv("/mnt/groupe1/Characters.csv", sep=";", header=True)
df = rawdata.toPandas()
#Ouvre le avec spark et fais un toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Objectif de l'EDA : Nettoyer, traiter et analyser le dataset Characters.

# COMMAND ----------

df.head()

# COMMAND ----------

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

nb_Male_Gryffon = sum(df[df["House"] == "Gryffindor"]["Gender"] == "Male")
nb_tot_Gryffon = len(df[df["House"] == "Gryffindor"]["Gender"])
nb_Female_Gryffon = nb_tot_Gryffon - nb_Male_Gryffon
print(round(nb_Male_Gryffon/nb_tot_Gryffon * 100,3),'%')

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC On observe qu'il y a 70 % d'hommes 

# COMMAND ----------

for house in list(df["House"].drop_duplicates()):
    nb_Male = sum(df[df["House"] == house]["Gender"] == "Male")
    nb_tot = len(df[df["House"] == house]["Gender"])
    nb_Female = nb_tot - nb_Male
    print(house)
    if(nb_tot > 0):
        print(round(nb_Male/nb_tot * 100,3),'%')
    print(nb_tot)


# COMMAND ----------

nb_Male = len(df[df["Gender"] == "Male"])
nb_tot = len(df)
nb_Female = nb_tot - nb_tot
print(round(nb_Male/nb_tot * 100,3),'%')

# COMMAND ----------

# MAGIC %md
# MAGIC Nous avons trouvés les 4 maisons d'Harry Potter mais aussi des données étiquetées 'Beauxbatons Academy of Magic', 'Durmstrang Institute' et 'None'

# COMMAND ----------

list(df["House"].drop_duplicates())

# Databricks notebook source
rawdata = spark.read.csv("/mnt/groupe1/Characters.csv",sep=';',header=True)

# COMMAND ----------

rawdata

# Databricks notebook source
if "/mnt/groupe1" not in str(dbutils.fs.ls('/mnt')):
    dbutils.fs.mount(
    source = "wasbs://groupe1@esgidatas.blob.core.windows.net",
    mount_point = "/mnt/groupe1",
    extra_configs = {"fs.azure.account.key.esgidatas.blob.core.windows.net":dbutils.secrets.get(scope = "ScopeESGI", key = "testH")})
    rawdata = spark.read.csv("/mnt/groupe1/Characters.csv",sep=';',header=True)
else:
    print("Dossier déjà monté")

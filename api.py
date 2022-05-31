# Databricks notebook source
import os
import requests
import numpy as np
import pandas as pd
import json 

def score_model(url,token,data_json):
    
    headers = {'Authorization': f'Bearer '+token}
    
    response = requests.request(method='POST', headers=headers, url=url, json=data_json)
    
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()

#mettre le contenu de json que vous voulez tester

json_test = json.loads("""[{
"Gender":"Male",
"BloodStatus":"Half-blood",
"Species":"Human",
"Patronus":"Phoenix",
"Death":"30 June, 1997"}]""")

#Mettre l'url de l'api que vous avez mis en service 
url_api = 'https://adb-8992331337369088.8.azuredatabricks.net/model/g1/1/invocations'
#Mettre le token que vous venez de créer dans ce String 
token = "dapi9c51aa5d83f462e253b15e672311e54d"

score_model(url_api,token,json_test)

# COMMAND ----------



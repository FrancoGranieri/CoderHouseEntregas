import requests
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import datetime
from datetime import datetime
from dotenv import load_dotenv
import os


url = "https://api.coincap.io/v2/assets"
response = requests.get(url)

body_dict = response.json()
data_json = body_dict['data']
df = pd.DataFrame(data_json)

df['date_time'] = datetime.now()
df_final = df.loc[:, ['id', 'name', 'priceUsd', 'date_time']]
df_final['id_date'] = df_final['id'].astype(str) + '-' + df_final['date_time'].astype(str)
df_final_final = df_final[['id_date','id', 'name', 'priceUsd', 'date_time']]
#print(df_final_final)

credentials = pd.read_csv('CREDENTIALS.csv', delimiter=',')
# credentials = pd.read_csv(r'C:\Users\TALIGENT\Desktop\Material para prácticas\CREDENTIALS.csv', delimiter=',')
DB_USER = credentials['DB_USER'][0]
DB_PASS = credentials['DB_PASS'][0]
# print(credentials)


# # try:
conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname='data-engineer-database',
        user=DB_USER,
        password=DB_PASS,
        port='5439'
    )
# # except Exception as e:
# #     print("Unable to connect to Redshift"),
# #     print(e)

cur = conn.cursor()
table_name = 'cripto_price_history'
columns = ['id_date','id', 'name', 'priceUsd', 'date_time']
values = [tuple(x) for x in df_final_final.to_numpy()]
query = f"insert into {table_name} ({','.join(columns)}) values%s"
cur.execute("BEGIN")
execute_values(cur,query,values)
    

cur.execute("COMMIT")
cur.close()
conn.close()

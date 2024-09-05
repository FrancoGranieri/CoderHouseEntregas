import requests
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import datetime
import os
from airflow import DAG
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

dag_path = os.getcwd()

default_args = {
    'owner': 'NicolasRivas',
    'start_date': datetime(2023,6,26),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='Bitcoin_ETL',
    default_args=default_args,
    description='Agrega data de Bitcoin de forma diaria',
    schedule_interval="@daily",
    catchup=False
)

def extraer_data(**kwargs):
   url = "https://api.coincap.io/v2/assets"
   response = requests.get(url)  
   ### Transformmo el JSON de respuesta de la api en un dataframe para manejar los datos ###
   body_dict = response.json()
   data_json = body_dict['data']
   df = pd.DataFrame(data_json)
   json_data = df.to_json()
   ### Reetorna el valor del pythonoperator como json
   kwargs['ti'].xcom_push(key='data', value=df.to_json())
   print(f"Pushed data to XCom: {json_data[:100]}")  # Log para verificar


def transformar_data(**kwargs):

    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='extraer_data', key='data')
    df = pd.read_json(df_json)

    ##Convierto el timestamp a string para concatenarlo a la PK    
    df['date_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    ##Armo la pk con el ID y el timestamp
    df['key_id_date_time'] = df['id'] + "|" + df['date_time']
    
    ##Armo tabla final con el DF
    df_final = df[['id', 'name', 'priceUsd', 'date_time', 'key_id_date_time']]
    df_json = df_final.to_json()
    print(f"El DF pasado a JSO es {df_json}")
    print(f"El ti es {ti}")
    respuesta= ti.xcom_push(key='transformed_data', value=df_json)
    print(f"El ti pusheado es  {ti.xcom_push(key='transformed_data', value=df_json)}")


def cargar_data(**kwargs):
   
   ti = kwargs['ti']
   print(f"El ti en cargar_data es {ti}")
   df_final = ti.xcom_pull(task_ids='transformar_data', key='transformed_data')
   print(f"El df pulleado es {df_final}")

   
#    df_final = pd.read_json(df_json)

   ## Traigo las credenciales de las variables de entorno
   load_dotenv()
   DB_USER = os.getenv('POSTGRES_USER')
   DB_PASS = os.getenv('POSTGRES_PASSWORD')
   DB_HOST = os.getenv('DB_HOST')
   DB_NAME = os.getenv('POSTGRES_DB')
   if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
        print("Error: one or more environment variables are missing.")
        return
   try:
        ##Creo el conector a la base de datos con las credenciales previamente leidas
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port='5439'
    )

    ##Inicializo el cursor 
    cur = conn.cursor()
    
    ##Tabla temporal para almacenar los nuevos datos
    new_data_table_name = 'nicolas_rivas_coderhouse.cripto_price_new_data'

    ##Tabla destino en la que vamos a persistir los datos
    destiny_table_name = 'nicolas_rivas_coderhouse.cripto_price_history'
    columns = ['id', 'name', 'priceUsd', 'date_time', 'key_id_date_time']
    values = [tuple(x) for x in df_final.to_numpy()]

    ######Creo todas las queries necesarias a ejecutar
    ##elimino datos viejos de la temporal
    truncate_table_query = f"truncate table {new_data_table_name}"
    ##Cargo datos actualizados de la api en la temporal
    insert_data_in_temp_query = f"insert into {new_data_table_name} ({','.join(columns)}) values%s"
    ##Elimino duplicados de la temporal viendo que key es igual a la key de la tabla destino
    delete_duplicates_query = f"delete from {new_data_table_name} using {destiny_table_name} where {destiny_table_name}.key_id_date_time = {new_data_table_name}.key_id_date_time"
    ##inserto los datos de la temporal sin duplicados en la tabla destino
    final_insert_query = f"insert into {destiny_table_name} ({','.join(columns)}) select {','.join(columns)} from {new_data_table_name}"

    ##Ejecuto todas las queries en orden.
    cur.execute(truncate_table_query)
    cur.execute("BEGIN")
    execute_values(cur,insert_data_in_temp_query,values)
    cur.execute(delete_duplicates_query)
    cur.execute(final_insert_query)

    #Commiteo los cambios, cierro conexiones y cursor.
    cur.execute("COMMIT")
    cur.close()
    conn.close()
   except psycopg2.Error as e:
        print(f"Error en la base de datos: {e}")
        if conn:
            conn.rollback()

data_extraction = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    dag=BC_dag,
)

data_transformation = PythonOperator(
    task_id='data_transformation',
    python_callable=transformar_data,
    dag=BC_dag,
)

data_load = PythonOperator(
    task_id='data_load',
    python_callable=cargar_data,
    dag=BC_dag,
)

data_extraction >> data_transformation >> data_load
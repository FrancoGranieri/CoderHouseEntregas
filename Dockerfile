##Bajo imagne de pyhon
FROM python:3.9-slim

#Variable de entorno de AirFlow
ENV AIRFLOW_HOME=/opt/AirFlow

#Creacion de directorio de Airflow
RUN mkdir -p $AIRFLOW_HOME

#Establecer el directorio de trabajo de airflow
WORKDIR $AIRFLOW_HOME

#Instalar apache airflow y otras dependencias
RUN pip install apache-airflow pandas python-dotenv pyarrow

#Copia los archivos de DAGs al directorio de airflow
COPY dags/ $AIRFLOW_HOME/dags/

#Exponer el puerto necesario para el webserver
EXPOSE 8080 

# Script de inicio para el contenedor
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Establecer el script de entrada
ENTRYPOINT ["/entrypoint.sh"]
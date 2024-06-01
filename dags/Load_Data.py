from pyspark.sql.functions import when, col, date_format
from pyspark.sql import SparkSession
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

def read_parquet_file():
    df_gab_abstr = spark.read.parquet("hdfs://namenode:9000/DataLake/gab_abstr.parquet")
    df_getvir = spark.read.parquet("hdfs://namenode:9000/DataLake/getvir.parquet")
    df_solde = spark.read.parquet("hdfs://namenode:9000/DataLake/solde.parquet")
    df_perimetre = spark.read.parquet("hdfs://namenode:9000/DataLake/perimetre.parquet")
    df_statut_connexion = spark.read.parquet("hdfs://namenode:9000/DataLake/statut_connexion.parquet") 
    df_getfac = spark.read.parquet("hdfs://namenode:9000/DataLake/getfac.parquet") 
    df_cptferm = spark.read.parquet("hdfs://namenode:9000/DataLake/cptferm.parquet")
    df_fermcpt = spark.read.parquet("hdfs://namenode:9000/DataLake/fermcpt.parquet")
    df_operations_agence = spark.read.parquet("hdfs://namenode:9000/DataLake/operations_agence.parquet")
    df_gab22 = spark.read.parquet("hdfs://namenode:9000/DataLake/gab22.parquet")
    df_yousr = spark.read.parquet("hdfs://namenode:9000/DataLake/yousr.parquet")
    return df_gab_abstr, df_getvir, df_solde, df_perimetre, df_statut_connexion, df_getfac, df_cptferm, df_fermcpt, df_operations_agence, df_gab22, df_yousr

def unify_client_ids(df_gab_abstr, df_getvir, df_solde, df_perimetre, df_statut_connexion, df_getfac, df_cptferm, df_fermcpt, df_operations_agence, df_gab22, df_yousr):
    # Unify the client ids in all tables
    df_gab_abstr = df_gab_abstr.withColumnRenamed("new", "Idclient")
    df_getvir = df_getvir.withColumnRenamed("new", "Idclient")
    df_solde = df_solde.withColumnRenamed("radical_new", "Idclient") 
    df_perimetre = df_perimetre.withColumnRenamed("radical_new", "Idclient") 
    df_statut_connexion = df_statut_connexion.withColumnRenamed("radical_new", "Idclient") 
    df_getfac = df_getfac.withColumnRenamed("radical_new", "Idclient") 
    df_cptferm = df_cptferm.withColumnRenamed("new", "Idclient") 
    df_fermcpt = df_fermcpt.withColumnRenamed("new", "Idclient")
    df_operations_agence = df_operations_agence.withColumnRenamed("new", "Idclient")
    df_gab22 = df_gab22.withColumnRenamed("new", "Idclient")
    df_yousr = df_yousr.withColumnRenamed("new", "Idclient")
    
    return df_gab_abstr, df_getvir, df_solde, df_perimetre, df_statut_connexion, df_getfac, df_cptferm, df_fermcpt, df_operations_agence, df_gab22, df_yousr

# Define the DAG
dag = DAG(
    'clean_perimetre_data',
    description='DAG for cleaning perimetre data',
    schedule_interval=None,
    start_date=datetime(2023, 6, 5),
    catchup=False
)

load_data_task = PythonOperator(
    task_id='load_perimetre_task',
    python_callable=read_parquet_file,
    dag=dag
)

unify_data_ids = PythonOperator(
    task_id='unify_data_ids',
    python_callable=unify_client_ids,
    op_kwargs={'df_gab_abstr': df_gab_abstr, 'df_getvir': df_getvir, 'df_solde': df_solde, 'df_perimetre': df_perimetre, 'df_statut_connexion': df_statut_connexion, 'df_getfac': df_getfac, 'df_cptferm': df_cptferm, 'df_fermcpt': df_fermcpt, 'df_operations_agence': df_operations_agence, 'df_gab22': df_gab22, 'df_yousr': df_yousr},
    dag=dag
)

# Define the dependencies between tasks
load_data_task >> unify_data_ids

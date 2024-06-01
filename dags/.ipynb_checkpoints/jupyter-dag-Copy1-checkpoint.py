# from pyspark.sql.functions import year, when, lit, floor, months_between, to_date
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator

# spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# def read_parquet_file(file_path, table_name):
#     global df_gab_abstr = spark.read.parquet("hdfs://namenode:9000/DataLake/gab_abstr.parquet")
#     global df_getvir = spark.read.parquet("hdfs://namenode:9000/DataLake/getvir.parquet")
#     global df_solde = spark.read.parquet("hdfs://namenode:9000/DataLake/solde.parquet")
#     global df_perimetre = spark.read.parquet("hdfs://namenode:9000/DataLake/perimetre.parquet")
#     global df_statut_connexion = spark.read.parquet("hdfs://namenode:9000/DataLake/statut_connexion.parquet") 
#     global df_getfac = spark.read.parquet("hdfs://namenode:9000/DataLake/getfac.parquet") 
#     global df_cptferm = spark.read.parquet("hdfs://namenode:9000/DataLake/cptferm.parquet")
#     global df_fermcpt = spark.read.parquet("hdfs://namenode:9000/DataLake/fermcpt.parquet")
#     global df_operations_agence = spark.read.parquet("hdfs://namenode:9000/DataLake/operations_agence.parquet")
#     global df_gab22 = spark.read.parquet("hdfs://namenode:9000/DataLake/gab22.parquet")
#     global df_yousr = spark.read.parquet("hdfs://namenode:9000/DataLake/yousr.parquet")

# def unify_client_ids():
#     # Unifier les ids clients dans tous les tables
#     df_gab_abstr = df_gab_abstr.withColumnRenamed("new", "Idclient")
#     df_getvir = df_getvir.withColumnRenamed("new", "Idclient")
#     df_solde = df_solde.withColumnRenamed("radical_new", "Idclient") 
#     df_solde = df_solde.withColumnRenamed("radical_new", "Idclient") 
#     df_perimetre = df_perimetre.withColumnRenamed("radical_new", "Idclient") 
#     df_statut_connexion = df_statut_connexion.withColumnRenamed("radical_new", "Idclient") 
#     df_getfac = df_getfac.withColumnRenamed("radical_new", "Idclient") 
#     df_cptferm = df_cptferm.withColumnRenamed("new", "Idclient") 
#     df_fermcpt = df_fermcpt.withColumnRenamed("new", "Idclient")
#     df_operations_agence = df_operations_agence.withColumnRenamed("new", "Idclient")
#     df_df_gab22 = df_operations_agence.withColumnRenamed("new", "Idclient")
#     df_yousr = df_yousr.withColumnRenamed("new", "Idclient")

# def clean_perimetre_data():
#     # Nettoyage de la table df_perimetre
#     df_perimetre = spark.sql("SELECT * FROM df_perimetre")
    
#     # Suppression des lignes avec des valeurs nulles dans les colonnes 'CODE_POSTAL' et 'VILLE'
#     df_perimetre = df_perimetre.filter(df_perimetre["CODE_POSTAL"].isNotNull() & df_perimetre["VILLE"].isNotNull())
    
#     # Calcul de la moyenne des années de naissance
#     mean_year = df_perimetre.select(year("DATE_NAISSANCE")).agg({"year(DATE_NAISSANCE)": "avg"}).collect()[0][0]
    
#     # Remplacement des valeurs nulles dans la colonne 'DATE_NAISSANCE' par la moyenne des années de naissance
#     df_perimetre = df_perimetre.withColumn("DATE_NAISSANCE", when(df_perimetre["DATE_NAISSANCE"].isNull(), lit(mean_year).cast("integer")).otherwise(df_perimetre["DATE_NAISSANCE"]))

# #group by age
# from pyspark.sql.functions import when
# df_perimetre = df_perimetre.withColumn("genre", when(df_perimetre["genre"] == "FÃ©minin", "Féminin").otherwise(df_perimetre["genre"]))

# #group by MARITAL_STATUS
# df_perimetre = df_perimetre.withColumn("MARITAL_STATUS", when(df_perimetre["MARITAL_STATUS"] == "MariÃ© (e)", "Marié(e)").when(df_perimetre["MARITAL_STATUS"] == "DivorcÃ© (e)", "Divorcé(e)").otherwise(df_perimetre["MARITAL_STATUS"]))

# #Colonne CUSTOMER_SINCE | DATE_NAISSANCE : Supprimer le temp  
# from pyspark.sql.functions import date_format
# df_perimetre = df_perimetre.withColumn("DATE_NAISSANCE", date_format(df_perimetre["DATE_NAISSANCE"], "yyyy-MM-dd"))
# df_perimetre = df_perimetre.withColumn("CUSTOMER_SINCE", date_format(df_perimetre["CUSTOMER_SINCE"], "yyyy-MM-dd"))

# # Calcul de la moyenne des années de naissance
# from pyspark.sql.functions import year, col, udf
# from pyspark.sql.types import StringType
# mean_year = df_perimetre.select(year("DATE_NAISSANCE")).agg({"year(DATE_NAISSANCE)": "avg"}).collect()[0][0]

# from pyspark.sql.functions import when, col
# df_perimetre = df_perimetre.withColumn('DATE_NAISSANCE', when(col('DATE_NAISSANCE').isNull(), '1975-01-01').otherwise(col('DATE_NAISSANCE')))

# #Colonne ville
# #supprimer 1 champ de Ville
# df_perimetre = df_perimetre.withColumn("VILLE", col("VILLE").member1)
# # show dict ville with 
# df_ville = df_perimetre.select("BPR", "VILLE")
# df_ville = df_ville.dropDuplicates()
# dict_ville = df_ville.rdd.collectAsMap()


# from pyspark.sql.functions import rand, when

# df_perimetre = df_perimetre.withColumn("genre", when(df_perimetre["genre"].isNull(), when(rand() < 0.5, "Féminin").otherwise("Masculin")).otherwise(df_perimetre["genre"]))
# df_perimetre = df_perimetre.withColumn("MARITAL_STATUS", when(df_perimetre["MARITAL_STATUS"].isNull(), when(rand() < 0.5, "Celibataire").otherwise("Marié(e)")).otherwise(df_perimetre["MARITAL_STATUS"]))
# df_perimetre.describe().show()
# df_perimetre.groupBy("BPR").count().show()
# #ce code calcule l'âge des clients (en années) à partir de leur date de naissance et la durée (en années) pendant laquelle ils sont devenus clients en utilisant la date de référence "2020-01-01". 
# #Les résultats sont ajoutés en tant que nouvelles colonnes dans le DataFrame df2_perimetre.
# from pyspark.sql.functions import datediff, months_between, to_date,lit
# df_perimetre = df_perimetre.withColumn("age", (months_between(to_date(lit("2020-01-01")), to_date("DATE_NAISSANCE"), True) / 12).cast("integer"))
# df_perimetre = df_perimetre.withColumn("CUSTOMER_YEARS", (months_between(to_date(lit("2020-01-01")), to_date("CUSTOMER_SINCE"), True) / 12).cast("integer"))
# # from pyspark.sql.functions import floor, months_between, to_date, lit
# # # Calculer le nombre de mois
# # mois = months_between(to_date(lit("2020-01-01")), to_date("CUSTOMER_SINCE"), True)

# # # Extraire les composantes année et mois
# # annees = floor(mois / 12)
# # mois_restants = floor(mois % 12)

# # # Créer une nouvelle colonne 'CUSTOMER_YEARS_MONTHS' combinant les années et les mois
# # df_perimetre = df_perimetre.withColumn("CUSTOMER_YEARS_MONTHS", lit(annees) + lit(",") + lit(mois_restants))
# # df_perimetre = df_perimetre.drop("CUSTOMER_YEARS_MONTHS")
# # df_perimetre.select("CUSTOMER_YEARS_MONTHS").show(10)
# # Filtrer les lignes avec des valeurs > 2020-01-01" dans le DataFrame
# df_perimetre_null = df_perimetre.filter(df_perimetre["CUSTOMER_SINCE"] > "2020-01-01")

# # Afficher les lignes avec des valeurs nulles
# df_perimetre_null.count()
# columns_to_describe = ["age", "CUSTOMER_YEARS"]
# df_perimetre.select(columns_to_describe).describe().show()
# #visualiser l'age en boxplot
# import matplotlib.pyplot as plt

# df_perimetre_pandas = df_perimetre.toPandas()

# # Crée un boxplot de l'âge
# plt.figure(figsize=(10, 6))
# plt.boxplot(df_perimetre_pandas['age'].values)
# plt.ylabel('Age (en années)')
# plt.title('Boxplot de l\'âge')
# plt.show()
# # Affiche les statistiques descriptives de l'âge
# df_perimetre_pandas['age'].describe()
# from pyspark.sql.functions import col

# # Calcule Q1, Q3 et IQR
# Q1 = df_perimetre.approxQuantile('age', [0.25], 0)[0]
# Q3 = df_perimetre.approxQuantile('age', [0.75], 0)[0]
# IQR = Q3 - Q1

# # Définit les limites pour les outliers
# lower_bound = Q1 - 1.5 * IQR
# upper_bound = Q3 + 1.5 * IQR

# # Supprime les outliers
# df_perimetre = df_perimetre.filter((col('age') >= lower_bound) & (col('age') <= upper_bound))
# df_perimetre.select('age').describe().show()
# df_perimetre_pandas = df_perimetre.toPandas()
# df_perimetre_pandas['age'].describe()
# # Crée un boxplot de l'âge
# plt.figure(figsize=(10, 6))
# plt.boxplot(df_perimetre_pandas['age'].values)
# plt.ylabel('Age (en années)')
# plt.title('Boxplot de l\'âge')
# plt.show()
# df_perimetre = df_perimetre.filter(col('age') >= 16)
# df_perimetre.select('age').describe().show()
# # Filtre le DataFrame où CUSTOMER_YEAR est supérieur à AGE
# df_perimetre = df_perimetre.filter((col('CUSTOMER_YEARS') < col('age')))
# # Calcule le nombre de lignes
# df_perimetre.count()
# columns_to_describe = ["age", "CUSTOMER_YEARS"]
# df_perimetre.select(columns_to_describe).describe().show()
# df_peri = df_perimetre
# # Supprime la colonne 'VILLE'
# df_perimetre = df_perimetre.drop('VILLE')
# from pyspark.sql.functions import col, create_map, lit
# from itertools import chain

# mapping_dic = {
#     '1': 'BP Centre Sud',
#     '17':'BCP Réseau',
#     '78':'BCP Réseau',
#     '27':'BP Fès Meknès',
#     '48':'BP Fès Meknès',
#     '43':'BP Lâayoune',
#     '45':'BP Marrakech Béni Mellal',
#     '50':'BP Nador El Hoceima',
#     '57':'BP Oujda',
#     '64':'BP Tanger Tétouan',
#     '81':'BP Rabat Kénitra',
#     '90':'BCP Réseau',
#     '30':'BP LIB ATTAWFIK',
#     '40':'BP LIB ATTAWFIK',
# }

# # Convertir le dictionnaire en une colonne Spark
# mapping_expr = create_map([lit(x) for x in chain(*mapping_dic.items())])
# df_perimetre = df_perimetre.withColumn('region', mapping_expr.getItem(col('BPR')))
# # check corespandance
# distinct_regions = df_perimetre.select('region', 'BPR').distinct()
# distinct_regions.show()
# df_perimetre.show(10)
# df_perimetre = df_perimetre.fillna({'region': 'BP Nador El Hoceima'})
# df_perimetre.show(5)
# df_perimetre.groupBy('region').count().show()
# from pyspark.sql.functions import when

# df_perimetre = df_perimetre.withColumn(
#     "AGE_GROUP",
#     when(df_perimetre.age < 25, "16-24")\
#     .when((df_perimetre.age >= 25) & (df_perimetre.age < 35), "25-34")\
#     .when((df_perimetre.age >= 35) & (df_perimetre.age < 45), "35-44")\
#     .when((df_perimetre.age >= 45) & (df_perimetre.age < 55), "45-54")\
#     .when((df_perimetre.age >= 55) & (df_perimetre.age < 65), "55-64")\
#     .otherwise("65+")
# )

# def clean_statut_connexion_data():
#     # Nettoyage de la table df_statut_connexion
#     df_statut_connexion = spark.sql("SELECT * FROM df_statut_connexion")
#     # Effectuer le nettoyage spécifique à cette table
#     df_statut_connexion_cleaned = ...
#     df_statut_connexion_cleaned.createOrReplaceTempView("df_statut_connexion")

# def clean_getfac_data():
#     # Nettoyage de la table df_getfac
#     df_getfac = spark.sql("SELECT * FROM df_getfac")
#     # Effectuer le nettoyage spécifique à cette table
#     df_getfac_cleaned = ...
#     df_getfac_cleaned.createOrReplaceTempView("df_getfac")

# def clean_gab_abstr_data():
#     # Nettoyage de la table df_gab_abstr
#     df_gab_abstr = spark.sql("SELECT * FROM df_gab_abstr")
#     # Effectuer le nettoyage spécifique à cette table
#     df_gab_abstr_cleaned = ...
#     df_gab_abstr_cleaned.createOrReplaceTempView("df_gab_abstr")

# def clean_getvir_data():
#     # Nettoyage de la table df_getvir
#     df_getvir = spark.sql("SELECT * FROM df_getvir")
#     # Effectuer le nettoyage spécifique à cette table
#     df_getvir_cleaned = ...
#     df_getvir_cleaned.createOrReplaceTempView("df_getvir")

# def clean_cptferm_data():
#     # Nettoyage de la table df_cptferm
#     df_cptferm = spark.sql("SELECT * FROM df_cptferm")
#     # Effectuer le nettoyage spécifique à cette table
#     df_cptferm_cleaned = ...
#     df_cptferm_cleaned.createOrReplaceTempView("df_cptferm")

# def clean_fermcpt_data():
#     # Nettoyage de la table df_fermcpt
#     df_fermcpt = spark.sql("SELECT * FROM df_fermcpt")
#     # Effectuer le nettoyage spécifique à cette table
#     df_fermcpt_cleaned = ...
#     df_fermcpt_cleaned.createOrReplaceTempView("df_fermcpt")

# def clean_solde_data():
#     # Nettoyage de la table df_solde
#     df_solde = spark.sql("SELECT * FROM df_solde")
#     # Effectuer le nettoyage spécifique à cette table
#     df_solde_cleaned = ...
#     df_solde_cleaned.createOrReplaceTempView("df_solde")

# def calculate_age():
#     # Calculer l'âge à partir de la date de naissance dans la table df_perimetre
#     df_perimetre = spark.sql("SELECT * FROM df_perimetre")
#     current_date = datetime.now().date()
#     df_perimetre = df_perimetre.withColumn("AGE", floor(months_between(to_date(lit(current_date)), to_date("DATE_NAISSANCE"))) / 12)
#     df_perimetre.createOrReplaceTempView("df_perimetre")

# def calculate_account_duration():
#     # Calculer la durée du compte à partir de la date d'ouverture dans la table df_perimetre
#     df_perimetre = spark.sql("SELECT * FROM df_perimetre")
#     current_date = datetime.now().date()
#     df_perimetre = df_perimetre.withColumn("ACCOUNT_DURATION", floor(months_between(to_date(lit(current_date)), to_date("DATE_OUVERTURE"))))
#     df_perimetre.createOrReplaceTempView("df_perimetre")

# def save_transformed_data():
#     # Sauvegarder les données transformées dans des fichiers Parquet
#     table_names = ["df_perimetre", "df_statut_connexion", "df_getfac", "df_gab_abstr", "df_getvir", "df_cptferm", "df_fermcpt", "df_solde"]
#     for table_name in table_names:
#         df = spark.sql(f"SELECT * FROM {table_name}")
#         df.write.mode("overwrite").parquet(f"{table_name}_transformed.parquet")

# # Définition du DAG
# dag = DAG(
#     "data_transformation_dag",
#     default_args={
#         "owner": "Brahim",
#         "start_date": datetime(2023, 6, 5),
#         "retries": 1,
#         "retry_delay": timedelta(minutes=5)
#     },
#     schedule_interval="@daily"
# )

# # Définition des tâches
# task_read_files = PythonOperator(
#     task_id="read_files",
#     python_callable=read_parquet_file,
#     op_kwargs={"file_path": file_path, "table_name": "df_gab_abstr"},
#     dag=dag
# )

# task_unify_client_ids = PythonOperator(
#     task_id="unify_client_ids",
#     python_callable=unify_client_ids,
#     dag=dag
# )

# task_clean_perimetre_data = PythonOperator(
#     task_id="clean_perimetre_data",
#     python_callable=clean_perimetre_data,
#     dag=dag
# )

# task_clean_statut_connexion_data = PythonOperator(
#     task_id="clean_statut_connexion_data",
#     python_callable=clean_statut_connexion_data,
#     dag=dag
# )

# task_clean_getfac_data = PythonOperator(
#     task_id="clean_getfac_data",
#     python_callable=clean_getfac_data,
#     dag=dag
# )

# task_clean_gab_abstr_data = PythonOperator(
#     task_id="clean_gab_abstr_data",
#     python_callable=clean_gab_abstr_data,
#     dag=dag
# )

# task_clean_getvir_data = PythonOperator(
#     task_id="clean_getvir_data",
#     python_callable=clean_getvir_data,
#     dag=dag
# )

# task_clean_cptferm_data = PythonOperator(
#     task_id="clean_cptferm_data",
#     python_callable=clean_cptferm_data,
#     dag=dag
# )

# task_clean_fermcpt_data = PythonOperator(
#     task_id="clean_fermcpt_data",
#     python_callable=clean_fermcpt_data,
#     dag=dag
# )

# task_clean_solde_data = PythonOperator(
#     task_id="clean_solde_data",
#     python_callable=clean_solde_data,
#     dag=dag
# )

# task_calculate_age = PythonOperator(
#     task_id="calculate_age",
#     python_callable=calculate_age,
#     dag=dag
# )

# task_calculate_account_duration = PythonOperator(
#     task_id="calculate_account_duration",
#     python_callable=calculate_account_duration,
#     dag=dag
# )

# task_save_transformed_data = PythonOperator(
#     task_id="save_transformed_data",
#     python_callable=save_transformed_data,
#     dag=dag
# )

# # Définition des dépendances entre les tâches
# task_read_files >> task_unify_client_ids >> task_clean_perimetre_data
# task_clean_perimetre_data >> task_clean_statut_connexion_data
# task_clean_perimetre_data >> task_clean_getfac_data
# task_clean_perimetre_data >> task_clean_gab_abstr_data
# task_clean_perimetre_data >> task_clean_getvir_data
# task_clean_perimetre_data >> task_clean_cptferm_data
# task_clean_perimetre_data >> task_clean_fermcpt_data
# task_clean_perimetre_data >> task_clean_solde_data
# task_clean_perimetre_data >> task_calculate_age
# task_clean_perimetre_data >> task_calculate_account_duration
# task_clean_statut_connexion_data >> task_save_transformed_data
# task_clean_getfac_data >> task_save_transformed_data
# task_clean_gab_abstr_data >> task_save_transformed_data
# task_clean_getvir_data >> task_save_transformed_data
# task_clean_cptferm_data >> task_save_transformed_data
# task_clean_fermcpt_data >> task_save_transformed_data
# task_clean_solde_data >> task_save_transformed_data
# task_calculate_age >> task_save_transformed_data
# task_calculate_account_duration >> task_save_transformed_data

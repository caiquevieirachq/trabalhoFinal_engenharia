# %%
from pyspark.sql import functions as f
from pyspark.sql import SparkSession


# %%
print('Iniciando uma SparkSession')

# %%
spark = SparkSession.builder.appName('police_data').getOrCreate()

# %%
print("Reading CSV file from S3...")

police_data = (
    spark
    .read
    .csv('s3://airflow-puc/MplsStops.csv', header=True, sep=",", inferSchema=True)
)

# %%
print('Imprimindo Schema')

police_data.printSchema()

# %%
police_data = (
    police_data
    .withColumnRenamed('_c0', 'id')
    .withColumnRenamed('idNum', 'id_num')
    .withColumnRenamed('date', 'data')
    .withColumnRenamed('problem', 'situacao')
    .withColumnRenamed('MDC', 'MDC')
    .withColumnRenamed('citationIssued', 'citation')
    .withColumnRenamed('personSearch', 'buscaPessoal')
    .withColumnRenamed('vehicleSearch', 'buscaVeiculo')
    .withColumnRenamed('preRace', 'preRace')
    .withColumnRenamed('gender', 'genero')
    .withColumnRenamed('lat', 'latitude')
    .withColumnRenamed('long', 'longitude')
    .withColumnRenamed('policePrecinct', 'delegacia')
    .withColumnRenamed('neighborhood', 'bairro')

)

# %%
police_data.show()

# %%
print('Tabela View')

police_data.createOrReplaceTempView('temp_police')

# %%
print('Criando indicador')

# 1 - Ocorrencias suspeitas por bairro
indicador1 = spark.sql("""
  SELECT bairro,
  COUNT(situacao) as Suspeitas
  FROM temp_police
  WHERE situacao = 'suspicious'
  GROUP BY bairro
  ORDER BY Suspeitas desc
""")


# %%
indicador1.show()

# %%
# 2 - Abordagens de negros por bairro em suspeita
indicador2 = spark.sql("""
  SELECT bairro,
  COUNT(situacao) as abordagens_negros
  FROM temp_police
  WHERE situacao = 'suspicious' and race = 'Black'
  GROUP BY bairro
  ORDER BY abordagens_negros desc
""")


# %%
indicador2.show()

# %%
# 3 - Abordagens de brancos por bairro em suspeita
indicador3 = spark.sql("""
  SELECT bairro,
  COUNT(situacao) as abordagens_brancos
  FROM temp_police
  WHERE situacao = 'suspicious' and race = 'White'
  GROUP BY bairro
  ORDER BY abordagens_brancos desc
""")


# %%
indicador3.show()

# %%
print('Escrevendo arquivos no formato parquet')
#Transformando em Parquet
(
    indicador1
    .write
    .format('parquet')
    .mode("append")
    .save("s3://airflow-puc/parquet/Indicador1/")
)
print('Indicador 1 escrito com Sucesso!')

# %%
#Transformando em Parquet
(
    indicador2
    .write
    .format('parquet')
    .mode("append")
    .save("s3://airflow-puc/parquet/Indicador2/")
)
print('Indicador 2 escrito com Sucesso!')

# %%
#Transformando em Parquet
(
    indicador3
    .write
    .format('parquet')
    .mode("append")
    .save("s3://airflow-puc/parquet/Indicador3/")
)
print('Indicador 3 escrito com Sucesso!')

# %%
df1 = (
    spark
    .read
    .parquet('s3://airflow-puc/parquet/Indicador1/')
)
df1.show()

# %%
df2 = (
    spark
    .read
    .parquet('s3://airflow-puc/parquet/Indicador2/')
)
df2.show()

# %%
df3 = (
    spark
    .read
    .parquet('s3://airflow-puc/parquet/Indicador3/')
)
df3.show()

# %%
df1.createOrReplaceTempView('df1')

# %%
df2.createOrReplaceTempView('df2')

# %%
df3.createOrReplaceTempView('df3')

# %%
joinedTable = spark.sql("""
   SELECT
   distinct(df1.bairro),
   df1.suspeitas,
   df2.abordagens_negros,
   df3.abordagens_brancos
   FROM df1
   LEFT OUTER JOIN df2
   ON df1.bairro = df2.bairro
   LEFT OUTER JOIN df3
   ON df1.bairro = df3.bairro
   ORDER BY suspeitas desc
""")

# %%
joinedTable.show()

# %%


#Transformando em Parquet
(
    joinedTable
    .write
    .format('parquet')
    .mode("append")
    .save("s3://airflow-puc/parquet/Final/")
)


# %%




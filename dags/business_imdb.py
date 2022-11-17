from pyspark.sql import functions as f
from pyspark.sql import SparkSession

print('Iniciando uma SparkSession')

spark = SparkSession.builder.appName('business').getOrCreate()

print("Reading CSV file from S3...")

business = (
    spark
    .read
    .csv('business-financial-data-june-2022-quarter-csv.csv', header=True, sep=";", inferSchema=True)
)

print('Imprimindo Schema')

business.printSchema()

print('Renomeando Colunas')

business = (
    business
    .withColumnRenamed('Series_reference', 'Series_reference')
    .withColumnRenamed('Period', 'Period')
    .withColumnRenamed('Data_value', 'Data_value')
    .withColumnRenamed('Suppressed', 'Suppressed')
    .withColumnRenamed('STATUS', 'STATUS')
    .withColumnRenamed('UNITS', 'UNITS')
    .withColumnRenamed('Magnitude', 'Magnitude')
    .withColumnRenamed('Subject', 'Subject')
    .withColumnRenamed('Group', 'Group')
    .withColumnRenamed('Series_title_1', 'Series_title_1')
    .withColumnRenamed('Series_title_2', 'Series_title_2')
    .withColumnRenamed('Series_title_3', 'Series_title_3')
    .withColumnRenamed('Series_title_4', 'Series_title_4')
    .withColumnRenamed('Series_title_5', 'Series_title_5'))

print('Formatando Colunas')


business.show()

print('Tabela View')

business.createOrReplaceTempView('temp_business')

print('Criando indicador')

# 1 - Dados financeiros da empresa
indicador1 = spark.sql("""
  SELECT
  director,
  COUNT(Data_value) as business
  FROM business
  GROUP BY director
  ORDER BY COUNT(Data_value) DESC
""")

# 2 - média imdb_rating por diretor
indicador2 = spark.sql("""
  SELECT
  director,
  mean(imdb_rating) as media_imdb_rating
  FROM business
  GROUP BY director
  ORDER BY mean(imdb_rating) DESC
""")

# 3 - Tempo do maior filme por diretor
indicador3 = spark.sql("""
  SELECT
  director,
  max(runtime) as maior_filme_min
  FROM business
  GROUP BY director
  ORDER BY max(runtime) DESC
""")

# Transformando em Parquet
(
    indicador1
    .write
    .format('parquet')
    .mode("append")
    .save("s3://microdados-256240406578/PARQUETs//indicador1")
)

# Transformando em Parquet
(
    indicador2
    .write
    .format('parquet')
    .mode("append")
    .save("s3://microdados-256240406578/PARQUETs//indicador2")
)

# Transformando em Parquet
(
    indicador3
    .write
    .format('parquet')
    .mode("append")
    .save("s3://microdados-256240406578/PARQUETs//indicador3")
)

df1 = (
    spark
    .read
    .parquet('s3://microdados-256240406578/PARQUETs//indicador1')
)

df2 = (
    spark
    .read
    .parquet('s3://microdados-256240406578/PARQUETs//indicador2')
)

df3 = (
    spark
    .read
    .parquet('s3://microdados-256240406578/PARQUETs//indicador3')
)

df1.createOrReplaceTempView('df1')
df2.createOrReplaceTempView('df2')
df3.createOrReplaceTempView('df3')

tabelafinal = spark.sql("""
   SELECT
   df1.*,
   df2.media_imdb_rating,
   df3.maior_filme_min
   FROM df1
   INNER JOIN df2
   ON df1.director = df2.director
   INNER JOIN df3
   ON df1.director = df3.director
""")

tabelafinal.show()

# Transformando em Parquet
(
    tabelafinal
    .write
    .format('parquet')
    .mode("append")
    .save("s3://microdados-256240406578/PARQUETs/tabelafinal")
)

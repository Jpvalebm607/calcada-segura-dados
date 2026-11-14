# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro — Calçada Segura
# MAGIC
# MAGIC Modelo dimensional para o que o MVP mostra:
# MAGIC
# MAGIC - **fato_problema_calcada**: um problema de calçada, seja reclamação no SP156 ou ocorrência do robô/visão
# MAGIC - **dim_tempo**: dia, mês, dia da semana e o clima do dia (chuva e temperatura)
# MAGIC - **dim_local**: subprefeitura
# MAGIC - **dim_tipo**: origem e categoria do problema
# MAGIC
# MAGIC **LGPD na Ouro:** sem logradouro nem número; coordenadas arredondadas (~100 m);
# MAGIC `operador_id` vira hash SHA-256 com sal. O sal é gerado e guardado no Volume `controle`, **nunca** no código nem no Git.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

import os
import secrets

# Sal do hash (LGPD): gerado uma vez e guardado no Volume, nunca no código nem no Git
CAMINHO_SAL = "/Volumes/workspace/gs/controle/sal_lgpd.txt"
if not os.path.exists(CAMINHO_SAL):
    with open(CAMINHO_SAL, "w") as f:
        f.write(secrets.token_hex(16))
SAL = open(CAMINHO_SAL).read().strip()

DB = "workspace.gs"
sp = spark.table(f"{DB}.prata_sp156")
oc = spark.table(f"{DB}.prata_ocorrencias")
clima = spark.table(f"{DB}.prata_clima")

# Célula de ~1 km (2 casas decimais) para cruzar robô x SP156 sem expor endereço
celula = lambda lat, lon: F.concat_ws("_", F.format_number(lat, 2), F.format_number(lon, 2))
sp = sp.withColumn("celula", celula(F.col("lat"), F.col("lon")))
oc = oc.withColumn("celula", celula(F.col("lat"), F.col("lon")))

# COMMAND ----------

# MAGIC %md ## Ocorrências do robô: tem reclamação no SP156 ali? E qual subprefeitura?

# COMMAND ----------

# 1) Tem reclamação? Mesma célula de ~1 km de alguma reclamação do SP156
celulas_sp156 = sp.select("celula").distinct().withColumn("tem_reclamacao_sp156", F.lit(True))

# 2) Qual subprefeitura? A mais comum no SP156 dentro de uma zona maior (~11 km),
#    para a ocorrência ganhar subprefeitura mesmo onde ninguém reclamou.
#    (No MVP, o RPA define a subprefeitura exata pelo polígono do GeoSampa.)
zona = lambda lat, lon: F.concat_ws("_", F.format_number(lat, 1), F.format_number(lon, 1))
w = Window.partitionBy("zona").orderBy(F.desc("n"))
mapa_zona = (
    sp.withColumn("zona", zona(F.col("lat"), F.col("lon")))
    .groupBy("zona", "subprefeitura").agg(F.count("*").alias("n"))
    .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
    .select("zona", F.col("subprefeitura").alias("sub_zona"))
)
oc = (
    oc.withColumn("zona", zona(F.col("lat"), F.col("lon")))
    .join(celulas_sp156, "celula", "left")
    .fillna({"tem_reclamacao_sp156": False})
    .join(mapa_zona, "zona", "left")
    .withColumn("subprefeitura", F.coalesce("sub_zona", F.lit("NAO IDENTIFICADA")))
    .drop("sub_zona", "zona")
)

# COMMAND ----------

# MAGIC %md ## Dimensões

# COMMAND ----------

datas = sp.select(F.col("data_abertura").alias("data")) \
    .union(oc.select(F.to_date("criado_em").alias("data"))) \
    .union(clima.select("data")).distinct().filter("data IS NOT NULL")

dim_tempo = (
    datas.join(clima, "data", "left")
    .select(
        F.date_format("data", "yyyyMMdd").cast("int").alias("sk_tempo"),
        "data",
        F.year("data").alias("ano"),
        F.month("data").alias("mes"),
        F.date_format("data", "E").alias("dia_semana"),
        F.dayofweek("data").isin(1, 7).alias("fim_de_semana"),
        "chuva_mm", "temp_max_c", "temp_min_c",
        F.when(F.col("chuva_mm") >= 10, "chuvoso").when(F.col("chuva_mm") > 0, "garoa")
         .when(F.col("chuva_mm") == 0, "seco").otherwise("sem dado").alias("faixa_chuva"),
    )
)

dim_local = (
    sp.select("subprefeitura").union(oc.select("subprefeitura")).distinct()
    .select(F.xxhash64("subprefeitura").alias("sk_local"), "subprefeitura")
)

dim_tipo = (
    sp.select(F.lit("sp156").alias("origem"), F.col("servico").alias("categoria"))
    .union(oc.select("origem", F.col("tipo").alias("categoria")))
    .distinct()
    .select(F.xxhash64("origem", "categoria").alias("sk_tipo"), "origem", "categoria")
)

# COMMAND ----------

# MAGIC %md ## Fato

# COMMAND ----------

fato_sp = sp.select(
    F.col("id_solicitacao").alias("id_problema"),
    F.lit("sp156").alias("origem"),
    F.col("servico").alias("categoria"),
    F.col("data_abertura").alias("data"),
    "subprefeitura", "celula", "status", "dias_atendimento",
    (F.col("data_finalizacao").isNotNull()).alias("finalizada"),
    F.lit(None).cast("double").alias("confianca"),
    F.lit(None).cast("double").alias("inclinacao_pct"),
    F.lit(None).cast("double").alias("vibracao_rms"),
    F.lit(True).alias("tem_reclamacao_sp156"),
    F.lit(None).cast("string").alias("operador_hash"),
    F.round("lat", 3).alias("lat_aprox"), F.round("lon", 3).alias("lon_aprox"),
)
fato_oc = oc.select(
    F.col("id_ocorrencia").alias("id_problema"),
    "origem",
    F.col("tipo").alias("categoria"),
    F.to_date("criado_em").alias("data"),
    "subprefeitura", "celula", "status",
    F.lit(None).cast("int").alias("dias_atendimento"),
    (F.col("status") == "resolvida").alias("finalizada"),
    "confianca", "inclinacao_pct", "vibracao_rms", "tem_reclamacao_sp156",
    F.when(F.col("operador_id").isNotNull(), F.sha2(F.concat("operador_id", F.lit(SAL)), 256)).alias("operador_hash"),
    F.round("lat", 3).alias("lat_aprox"), F.round("lon", 3).alias("lon_aprox"),
)

fato = (
    fato_sp.unionByName(fato_oc)
    .withColumn("sk_tempo", F.date_format("data", "yyyyMMdd").cast("int"))
    .withColumn("sk_local", F.xxhash64("subprefeitura"))
    .withColumn("sk_tipo", F.xxhash64("origem", "categoria"))
    .select("id_problema", "sk_tempo", "sk_local", "sk_tipo", "status", "finalizada",
            "dias_atendimento", "confianca", "inclinacao_pct", "vibracao_rms",
            "tem_reclamacao_sp156", "operador_hash", "celula", "lat_aprox", "lon_aprox")
)

# A Ouro é recalculada inteira a partir da Prata a cada execução: idempotente
for nome, df in [("dim_tempo", dim_tempo), ("dim_local", dim_local), ("dim_tipo", dim_tipo),
                 ("fato_problema_calcada", fato)]:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DB}.{nome}")
    print(nome, spark.table(f"{DB}.{nome}").count())

# COMMAND ----------

# MAGIC %md ## Tabela que o MVP consome + exportação para a API

# COMMAND ----------

indicadores = spark.sql(f"""
    SELECT l.subprefeitura,
           COUNT_IF(t.origem = 'sp156')                                   AS reclamacoes_sp156,
           ROUND(AVG(IF(t.origem = 'sp156', f.dias_atendimento, NULL)), 1) AS dias_medios_atendimento,
           ROUND(100 * AVG(IF(t.origem = 'sp156', CAST(f.finalizada AS INT), NULL)), 1) AS pct_finalizadas,
           COUNT_IF(t.origem IN ('robo', 'visao'))                        AS ocorrencias_robo,
           COUNT_IF(t.origem IN ('robo', 'visao') AND NOT f.tem_reclamacao_sp156) AS ocorrencias_sem_reclamacao
    FROM {DB}.fato_problema_calcada f
    JOIN {DB}.dim_local l USING (sk_local)
    JOIN {DB}.dim_tipo  t USING (sk_tipo)
    GROUP BY l.subprefeitura
    ORDER BY reclamacoes_sp156 DESC
""")
indicadores.write.mode("overwrite").saveAsTable(f"{DB}.ouro_indicadores_subprefeitura")

caminho = "/Volumes/workspace/gs/exportacao/indicadores_subprefeitura.json"
indicadores.toPandas().to_json(caminho, orient="records", force_ascii=False, indent=1)
print("Exportado para", caminho)
display(indicadores)

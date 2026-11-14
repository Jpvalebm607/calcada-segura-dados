# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Calçada Segura
# MAGIC
# MAGIC Lê os arquivos que o Airflow deixou em `chegada/` e foram enviados ao Volume,
# MAGIC **só os novos** (Auto Loader com checkpoint), e grava as tabelas Bronze sem tratar nada.
# MAGIC
# MAGIC Como rodar para os prints do requisito 05:
# MAGIC 1. Depois de subir o **lote 1**: etapa = `lote1` → Run all
# MAGIC 2. Depois de subir o **lote 2**: etapa = `lote2` → Run all (só os arquivos novos entram)
# MAGIC 3. Sem subir nada: etapa = `reexecucao` → Run all (as contagens não mudam: não duplica)

# COMMAND ----------

dbutils.widgets.text("etapa", "lote1", "Etapa (lote1, lote2, reexecucao)")
ETAPA = dbutils.widgets.get("etapa")

CATALOGO, SCHEMA = "workspace", "gs"
DB = f"{CATALOGO}.{SCHEMA}"
BASE = f"/Volumes/{CATALOGO}/{SCHEMA}/chegada"
CONTROLE = f"/Volumes/{CATALOGO}/{SCHEMA}/controle"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DB}.chegada")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DB}.controle")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DB}.exportacao")
print("Etapa:", ETAPA)

# COMMAND ----------

import re
import unicodedata
from pyspark.sql import functions as F


def normalizar(nome: str) -> str:
    """'Data de Abertura' -> 'data_de_abertura' (Delta não aceita espaço/acento no nome)."""
    s = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()


def contar(tabela: str) -> int:
    return spark.table(tabela).count() if spark.catalog.tableExists(tabela) else 0


def carregar(fonte: str, formato: str, opcoes: dict) -> None:
    tabela = f"{DB}.bronze_{fonte}"
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.schemaLocation", f"{CONTROLE}/schema_{fonte}")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .options(**opcoes)
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
    )
    df = df.select([F.col(f"`{c}`").alias(normalizar(c)) for c in df.columns])
    (
        df.writeStream
        .option("checkpointLocation", f"{CONTROLE}/checkpoint_{fonte}")
        .trigger(availableNow=True)
        .toTable(tabela)
        .awaitTermination()
    )


FONTES = {
    "clima": ("json", {"multiLine": "true"}),
    "ocorrencias": ("json", {"multiLine": "true"}),
    # SP156 entra como linha bruta (texto); a Prata separa os campos pelo ";"
    "sp156": ("text", {"cloudFiles.schemaEvolutionMode": "none"}),
}

# COMMAND ----------

# MAGIC %md ## Contagens antes, carga incremental e contagens depois

# COMMAND ----------

linhas = []
for fonte, (formato, opcoes) in FONTES.items():
    tabela = f"{DB}.bronze_{fonte}"
    antes = contar(tabela)
    carregar(fonte, formato, opcoes)
    depois = contar(tabela)
    linhas.append((ETAPA, tabela, antes, depois, depois - antes))

resultado = spark.createDataFrame(
    linhas, "etapa string, tabela string, linhas_antes long, linhas_depois long, novas long"
).withColumn("executado_em", F.current_timestamp())
resultado.write.mode("append").saveAsTable(f"{DB}.bronze_contagens")
display(resultado)

# COMMAND ----------

# MAGIC %md ## Histórico de contagens (print do requisito 05)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {DB}.bronze_contagens ORDER BY executado_em, tabela"))

# COMMAND ----------

# MAGIC %md ## Arquivos já carregados por tabela (cada arquivo entra uma vez só)

# COMMAND ----------

for fonte in FONTES:
    display(spark.sql(f"""
        SELECT '{fonte}' AS fonte, arquivo_origem, COUNT(*) AS linhas, MIN(carregado_em) AS carregado_em
        FROM {DB}.bronze_{fonte} GROUP BY arquivo_origem ORDER BY carregado_em
    """))

# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata — Calçada Segura
# MAGIC
# MAGIC Da Bronze (tudo texto) para a Prata: **tipos corretos, nulos e inválidos tratados,
# MAGIC regras de qualidade com o total barrado por cada uma, remoção de duplicados e MERGE pela chave.**
# MAGIC Rodar de novo não duplica nada: o MERGE atualiza o que já existe.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

DB = "workspace.gs"
SP_BBOX = dict(lat_min=-24.01, lat_max=-23.35, lon_min=-46.83, lon_max=-46.36)
TIPOS = ["buraco", "calcada_quebrada", "obstaculo", "rampa_irregular", "trepidacao", "degrau"]

log_regras = []


def em_sp(lat, lon):
    return lat.between(SP_BBOX["lat_min"], SP_BBOX["lat_max"]) & lon.between(SP_BBOX["lon_min"], SP_BBOX["lon_max"])


def aplicar_regras(df, fonte: str, regras: dict):
    """Aplica as regras em ordem. Linha que falha (ou dá nulo) é barrada e contada."""
    for nome, condicao in regras.items():
        ok = F.coalesce(condicao, F.lit(False))
        barrados = df.filter(~ok).count()
        log_regras.append((fonte, nome, barrados))
        df = df.filter(ok)
    return df


def remover_duplicados(df, fonte: str, chave: list, ordem):
    antes = df.count()
    w = Window.partitionBy(*chave).orderBy(ordem.desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    log_regras.append((fonte, "duplicados_removidos", antes - df.count()))
    return df


def merge(df, tabela: str, chave: list):
    if not spark.catalog.tableExists(tabela):
        df.limit(0).write.saveAsTable(tabela)
    df.createOrReplaceTempView("lote_prata")
    cond = " AND ".join(f"alvo.{k} = novo.{k}" for k in chave)
    spark.sql(f"""
        MERGE INTO {tabela} AS alvo USING lote_prata AS novo ON {cond}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(tabela, "->", spark.table(tabela).count(), "linhas")

# COMMAND ----------

# MAGIC %md ## Clima (Open-Meteo): listas paralelas → uma linha por dia

# COMMAND ----------

b = spark.table(f"{DB}.bronze_clima")
# Na Bronze tudo é texto: "daily" chega como string JSON e é convertido aqui
ESQUEMA_DAILY = ("struct<time:array<string>, precipitation_sum:array<double>, "
                 "temperature_2m_max:array<double>, temperature_2m_min:array<double>>")
b = b.withColumn("daily", F.from_json("daily", ESQUEMA_DAILY))
clima = (
    b.select("carregado_em", "daily", F.posexplode("daily.time").alias("i", "data_txt"))
    .select(
        F.to_date("data_txt").alias("data"),
        F.expr("daily.precipitation_sum[i]").cast("double").alias("chuva_mm"),
        F.expr("daily.temperature_2m_max[i]").cast("double").alias("temp_max_c"),
        F.expr("daily.temperature_2m_min[i]").cast("double").alias("temp_min_c"),
        "carregado_em",
    )
)
clima = aplicar_regras(clima, "clima", {
    "data_valida": F.col("data").isNotNull(),
    "chuva_nao_negativa": F.col("chuva_mm") >= 0,
    "temp_max_maior_que_min": F.col("temp_max_c") >= F.col("temp_min_c"),
})
clima = remover_duplicados(clima, "clima", ["data"], F.col("carregado_em"))
merge(clima.drop("carregado_em"), f"{DB}.prata_clima", ["data"])

# COMMAND ----------

# MAGIC %md ## SP156: solicitações de calçada

# COMMAND ----------

CAMPOS_SP156 = ["data_de_abertura", "data_de_finalizacao", "meio_de_comunicacao", "tema", "assunto",
                "servico", "status", "orgao", "logradouro", "numero", "cep", "latitude", "longitude",
                "prefeitura_operacional", "distrito"]
ESQUEMA_SP156 = ", ".join(f"{c} STRING" for c in CAMPOS_SP156)

# Bronze guarda a linha bruta (com o cabeçalho de cada arquivo, que sai aqui).
b = spark.table(f"{DB}.bronze_sp156").filter(~F.lower("value").startswith("data de abertura"))
# A Prefeitura muda o formato entre trimestres:
#   3º TRI 2026 -> separador "," e data no padrão americano (MM/dd/yyyy)
#   2º TRI 2026 -> separador ";" e data no padrão brasileiro (dd/MM/yyyy)
b = b.withColumn("ponto_virgula", F.col("value").contains(";"))
linha = F.when(F.col("ponto_virgula"), F.from_csv("value", ESQUEMA_SP156, {"sep": ";"})) \
         .otherwise(F.from_csv("value", ESQUEMA_SP156, {"sep": ","}))
b = b.select(linha.alias("c"), "carregado_em", "ponto_virgula").select("c.*", "carregado_em", "ponto_virgula")


def data_sp156(coluna):
    return F.when(F.col("ponto_virgula"), F.expr(f"try_to_date({coluna}, 'dd/MM/yyyy')")) \
            .otherwise(F.expr(f"try_to_date({coluna}, 'MM/dd/yyyy')"))


sp = b.select(
    data_sp156("data_de_abertura").alias("data_abertura"),
    data_sp156("data_de_finalizacao").alias("data_finalizacao"),
    F.trim("meio_de_comunicacao").alias("canal"),
    F.trim("assunto").alias("assunto"),
    F.trim("servico").alias("servico"),
    F.trim("status").alias("status"),
    F.trim("orgao").alias("orgao"),
    F.trim("logradouro").alias("logradouro"),
    F.trim("numero").alias("numero"),
    F.expr("try_cast(replace(latitude, ',', '.') AS DOUBLE)").alias("lat"),
    F.expr("try_cast(replace(longitude, ',', '.') AS DOUBLE)").alias("lon"),
    F.upper(F.trim("prefeitura_operacional")).alias("subprefeitura"),
    F.upper(F.trim("distrito")).alias("distrito"),
    "carregado_em",
)
# O SP156 aberto não traz protocolo: a chave é um hash dos campos que identificam a solicitação
sp = sp.withColumn("id_solicitacao", F.sha2(F.concat_ws(
    "|", "data_abertura", "servico", "logradouro", "numero", "lat", "lon"), 256))
sp = sp.withColumn("dias_atendimento", F.datediff("data_finalizacao", "data_abertura"))

sp = aplicar_regras(sp, "sp156", {
    "data_abertura_valida": F.col("data_abertura").isNotNull(),
    "coordenada_dentro_de_sp": em_sp(F.col("lat"), F.col("lon")),
    "finalizacao_apos_abertura": F.col("data_finalizacao").isNull() | (F.col("dias_atendimento") >= 0),
})
sp = remover_duplicados(sp, "sp156", ["id_solicitacao"], F.col("carregado_em"))
merge(sp.drop("carregado_em"), f"{DB}.prata_sp156", ["id_solicitacao"])

# COMMAND ----------

# MAGIC %md ## Ocorrências do robô e da visão

# COMMAND ----------

b = spark.table(f"{DB}.bronze_ocorrencias")
col = lambda nome: F.col(nome) if nome in b.columns else F.lit(None).cast("string")
oc = b.select(
    F.col("id").alias("id_ocorrencia"),
    F.lower(F.trim("origem")).alias("origem"),
    F.lower(F.trim("tipo")).alias("tipo"),
    F.col("lat").cast("double").alias("lat"),
    F.col("lon").cast("double").alias("lon"),
    col("foto_url").alias("foto_url"),
    F.col("confianca").cast("double").alias("confianca"),
    F.col("inclinacao_pct").cast("double").alias("inclinacao_pct"),
    F.col("vibracao_rms").cast("double").alias("vibracao_rms"),
    F.to_timestamp("criado_em").alias("criado_em"),
    F.coalesce(F.lower("status"), F.lit("nova")).alias("status"),
    col("operador_id").alias("operador_id"),
    F.coalesce(col("simulado").cast("boolean"), F.lit(False)).alias("simulado"),
    "carregado_em",
)
oc = aplicar_regras(oc, "ocorrencias", {
    "tipo_valido": F.col("tipo").isin(TIPOS),
    "confianca_entre_0_e_1": F.col("confianca").isNull() | F.col("confianca").between(0, 1),
    "coordenada_dentro_de_sp": em_sp(F.col("lat"), F.col("lon")),
})
oc = remover_duplicados(oc, "ocorrencias", ["id_ocorrencia"], F.col("carregado_em"))
merge(oc.drop("carregado_em"), f"{DB}.prata_ocorrencias", ["id_ocorrencia"])

# COMMAND ----------

# MAGIC %md ## Total barrado por regra (print do requisito 06)

# COMMAND ----------

qualidade = spark.createDataFrame(log_regras, "fonte string, regra string, barrados long") \
    .withColumn("executado_em", F.current_timestamp())
qualidade.write.mode("append").saveAsTable(f"{DB}.prata_qualidade")
display(qualidade)

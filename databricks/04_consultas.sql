-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Consultas SQL da Ouro — Calçada Segura
-- MAGIC Cinco perguntas que o MVP responde ao cidadão. A **5ª é a de impacto**.
-- MAGIC Depois de rodar, escreva embaixo de cada resultado 1 ou 2 frases interpretando o número.

-- COMMAND ----------

-- MAGIC %md ## 1. Quais subprefeituras têm mais reclamações de calçada?

-- COMMAND ----------

SELECT l.subprefeitura, COUNT(*) AS reclamacoes
FROM workspace.gs.fato_problema_calcada f
JOIN workspace.gs.dim_local l USING (sk_local)
JOIN workspace.gs.dim_tipo t USING (sk_tipo)
WHERE t.origem = 'sp156'
GROUP BY l.subprefeitura
ORDER BY reclamacoes DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md ## 2. Quanto tempo a Prefeitura leva para atender, por tipo de pedido?

-- COMMAND ----------

SELECT t.categoria AS servico,
       COUNT(*) AS pedidos,
       ROUND(100 * AVG(CAST(f.finalizada AS INT)), 1) AS pct_finalizados,
       ROUND(AVG(f.dias_atendimento), 1) AS dias_medios,
       PERCENTILE_APPROX(f.dias_atendimento, 0.5) AS dias_mediana
FROM workspace.gs.fato_problema_calcada f
JOIN workspace.gs.dim_tipo t USING (sk_tipo)
WHERE t.origem = 'sp156'
GROUP BY t.categoria
ORDER BY pedidos DESC;

-- COMMAND ----------

-- MAGIC %md ## 3. Chove mais, reclama mais? Média de reclamações por dia em cada faixa de chuva

-- COMMAND ----------

WITH reclamacoes AS (
  SELECT f.sk_tempo, f.id_problema
  FROM workspace.gs.fato_problema_calcada f
  JOIN workspace.gs.dim_tipo t USING (sk_tipo)
  WHERE t.origem = 'sp156'
),
periodo AS (
  SELECT MIN(data_abertura) AS ini, MAX(data_abertura) AS fim FROM workspace.gs.prata_sp156
),
por_dia AS (
  SELECT d.data, d.faixa_chuva, COUNT(r.id_problema) AS reclamacoes
  FROM workspace.gs.dim_tempo d
  CROSS JOIN periodo p
  LEFT JOIN reclamacoes r ON r.sk_tempo = d.sk_tempo
  WHERE d.data BETWEEN p.ini AND p.fim
  GROUP BY d.data, d.faixa_chuva
)
SELECT faixa_chuva, COUNT(*) AS dias, ROUND(AVG(reclamacoes), 1) AS reclamacoes_por_dia
FROM por_dia
GROUP BY faixa_chuva
ORDER BY reclamacoes_por_dia DESC;

-- COMMAND ----------

-- MAGIC %md ## 4. Que problemas o robô e a câmera encontram, e onde?

-- COMMAND ----------

SELECT l.subprefeitura, t.categoria AS tipo, COUNT(*) AS ocorrencias,
       ROUND(AVG(f.confianca), 2) AS confianca_media,
       ROUND(MAX(f.inclinacao_pct), 1) AS maior_inclinacao_pct
FROM workspace.gs.fato_problema_calcada f
JOIN workspace.gs.dim_local l USING (sk_local)
JOIN workspace.gs.dim_tipo t USING (sk_tipo)
WHERE t.origem IN ('robo', 'visao')
GROUP BY l.subprefeitura, t.categoria
ORDER BY ocorrencias DESC;

-- COMMAND ----------

-- MAGIC %md ## 5. IMPACTO: quantos problemas o robô achou onde ninguém tinha reclamado?
-- MAGIC Problemas "invisíveis" para a Prefeitura: sem nenhuma reclamação no SP156 na mesma célula de ~1 km.
-- MAGIC Rampas acima de 8,33% (NBR 9050) aparecem separadas porque afetam diretamente cadeirantes.

-- COMMAND ----------

SELECT COUNT(*) AS ocorrencias_robo,
       COUNT_IF(NOT f.tem_reclamacao_sp156) AS sem_reclamacao_sp156,
       ROUND(100 * COUNT_IF(NOT f.tem_reclamacao_sp156) / COUNT(*), 1) AS pct_invisiveis,
       COUNT_IF(f.inclinacao_pct > 8.33) AS rampas_fora_da_nbr9050
FROM workspace.gs.fato_problema_calcada f
JOIN workspace.gs.dim_tipo t USING (sk_tipo)
WHERE t.origem IN ('robo', 'visao');

"""DAG de coleta do Calçada Segura (GS FIAP 2026.2 - Big Data).

inicio >> [clima, sp156, ocorrencias] >> conferir_chegada >> fim

- 3 coletas em paralelo (requisito 03)
- 1 tarefa que confere se os arquivos chegaram (requisito 03)
- chaves e URLs em Airflow Variables, nunca no código (requisito 03)
- notificação: falha de qualquer tarefa e sucesso do lote avisam no
  webhook da Variable NOTIFY_WEBHOOK (Discord/Slack); sem ela, vai pro log
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

from coletas import fontes

CHEGADA = Path(os.environ.get("CHEGADA_DIR", "/opt/airflow/chegada"))
log = logging.getLogger(__name__)


def _var(nome: str) -> str | None:
    try:
        valor = Variable.get(nome, default=None)
    except Exception:  # Variable inexistente
        valor = None
    return valor or None


def _avisar(texto: str) -> None:
    url = _var("NOTIFY_WEBHOOK")
    log.info("[aviso] %s", texto)
    if url:
        try:
            requests.post(url, json={"content": texto, "text": texto}, timeout=15)
        except Exception as e:  # aviso não pode derrubar a tarefa
            log.warning("Falha ao notificar: %s", e)


def avisar_falha(context) -> None:
    ti = context["task_instance"]
    _avisar(f"❌ gs_coleta falhou: tarefa {ti.task_id}, dia {context['ds']}")


def avisar_sucesso(context) -> None:
    _avisar(f"✅ gs_coleta ok: lote do dia {context['ds']} completo em chegada/")


def t_clima(ds, **_):
    return fontes.coletar_clima(ds, CHEGADA)


def t_sp156(ds, **_):
    return fontes.coletar_sp156(ds, CHEGADA, url_csv=_var("SP156_CSV_URL"))


def t_ocorrencias(ds, **_):
    return fontes.coletar_ocorrencias(ds, CHEGADA, api_url=_var("API_URL"), api_key=_var("API_KEY"))


def t_conferir(ds, **_):
    return fontes.conferir_chegada(ds, CHEGADA)


with DAG(
    dag_id="gs_coleta",
    description="Coleta diária: Open-Meteo + SP156 + ocorrências do robô",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 12, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": avisar_falha,
    },
    tags=["gs", "calcada-segura"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    clima = PythonOperator(task_id="coletar_clima", python_callable=t_clima)
    sp156 = PythonOperator(task_id="coletar_sp156", python_callable=t_sp156)
    ocorrencias = PythonOperator(task_id="coletar_ocorrencias", python_callable=t_ocorrencias)
    conferir = PythonOperator(
        task_id="conferir_chegada",
        python_callable=t_conferir,
        on_success_callback=avisar_sucesso,
    )
    fim = EmptyOperator(task_id="fim")

    inicio >> [clima, sp156, ocorrencias] >> conferir >> fim

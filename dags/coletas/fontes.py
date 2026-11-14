"""Funções de coleta do Calçada Segura.

Sem import do Airflow aqui: assim dá para testar cada função com `python -m`
fora do Airflow. O DAG (gs_coleta.py) só chama estas funções.

Cada coleta grava UM arquivo por execução em chegada/<fonte>/<fonte>_<ds>.<ext>,
com a data lógica (ds) no nome, como pede o requisito 02.
"""
from __future__ import annotations

import csv
import io
import json
import random
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import requests

TIMEOUT = 60

# Centro aproximado de SP (Praça da Sé) e caixa que cobre o município
SP_LAT, SP_LON = -23.5505, -46.6333
SP_BBOX = {"lat_min": -24.01, "lat_max": -23.35, "lon_min": -46.83, "lon_max": -46.36}

CKAN_SP = "https://dados.prefeitura.sp.gov.br/api/3/action/package_show"
PACOTE_SP156 = "dados-do-sp156"

TIPOS = ["buraco", "calcada_quebrada", "obstaculo", "rampa_irregular", "trepidacao", "degrau"]


def _destino(chegada: Path, fonte: str, ds: str, ext: str) -> Path:
    pasta = Path(chegada) / fonte
    pasta.mkdir(parents=True, exist_ok=True)
    return pasta / f"{fonte}_{ds}.{ext}"


# ---------------------------------------------------------------------------
# Fonte 1 - Open-Meteo (externa, sem chave)
# ---------------------------------------------------------------------------
INICIO_HISTORICO = "2026-01-01"


def coletar_clima(ds: str, chegada: Path) -> str:
    """Chuva e temperatura diárias de SP, de 01/01/2026 até 5 dias antes de ds.

    Usa a API de arquivo (archive) do Open-Meteo, que tem ~5 dias de atraso.
    Trazer o histórico permite cruzar a chuva com as reclamações do SP156,
    que chegam por trimestre. A Prata faz MERGE pela data, então os dias que
    se repetem entre coletas não duplicam.
    """
    fim = (datetime.fromisoformat(ds) - timedelta(days=5)).date().isoformat()
    r = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": SP_LAT,
            "longitude": SP_LON,
            "start_date": INICIO_HISTORICO,
            "end_date": fim,
            "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min",
            "timezone": "America/Sao_Paulo",
        },
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    destino = _destino(chegada, "clima", ds, "json")
    destino.write_text(r.text, encoding="utf-8")
    return str(destino)


# ---------------------------------------------------------------------------
# Fonte 2 - SP156 (dados abertos da Prefeitura, CKAN)
# ---------------------------------------------------------------------------
def _url_csv_mais_recente() -> str:
    """Pergunta ao CKAN da Prefeitura qual é o CSV de solicitações mais recente.

    Só considera recursos "Dados do SP156 - ..." em CSV (ignora dicionários,
    releases em PDF e a pesquisa "Avaliação dos canais").
    """
    r = requests.get(CKAN_SP, params={"id": PACOTE_SP156}, timeout=TIMEOUT)
    r.raise_for_status()
    try:
        recursos = r.json()["result"]["resources"]
    except ValueError as e:
        raise RuntimeError(
            "O portal da Prefeitura não devolveu JSON (API fora ou bloqueada). "
            "Cadastre o link do CSV na Variable SP156_CSV_URL."
        ) from e
    recursos = [
        x for x in recursos
        if (x.get("name") or "").lower().startswith("dados do sp156")
        and ((x.get("format") or "").upper() == "CSV" or x.get("url", "").lower().endswith(".csv"))
    ]
    if not recursos:
        raise RuntimeError("Nenhum CSV 'Dados do SP156' no pacote: use a Variable SP156_CSV_URL")
    def periodo(x):  # "Dados do SP156 - 2º TRI 2026" -> (2026, 2)
        m = re.search(r"(\d)\D{0,3}tri\D*(\d{4})", (x.get("name") or "").lower())
        return (int(m.group(2)), int(m.group(1))) if m else (0, 0)

    recursos.sort(key=periodo, reverse=True)
    return recursos[0]["url"]


# Serviços do SP156 que são, de fato, sobre o estado da calçada.
# (Sem isso o filtro pegava ~16 mil pedidos de poda de árvore "em calçadas".)
SERVICOS_CALCADA = (
    "calçada pública",
    "calçada particular",
    "obstáculo na calçada",
    "construção de calçadas",
)


def filtrar_calcadas(linhas, saida, sep: str | None = None) -> tuple[int, int]:
    """Copia o cabeçalho e só as solicitações cujo Serviço é sobre calçada.

    Filtrar na coleta deixa os lotes leves (~4 mil de ~460 mil linhas por
    trimestre). A regra fica documentada no README. Retorna (lidas, mantidas).
    """
    # A Prefeitura alterna o separador entre trimestres ("," ou ";"): detecta
    # pelo cabeçalho e grava sempre com ";" para padronizar a chegada
    linhas = iter(linhas)
    primeira = next(linhas)
    if sep is None:
        sep = ";" if primeira.count(";") >= primeira.count(",") else ","
    import itertools
    leitor = csv.reader(itertools.chain([primeira], linhas), delimiter=sep)
    escritor = csv.writer(saida, delimiter=";", lineterminator="\n")
    cabecalho = next(leitor)
    escritor.writerow(cabecalho)
    nomes = [c.strip().lower() for c in cabecalho]
    i_serv = next((i for i, c in enumerate(nomes) if c.startswith("servi")), None)
    lidas = mantidas = 0
    for row in leitor:
        lidas += 1
        alvo = (row[i_serv] if i_serv is not None and i_serv < len(row) else ";".join(row)).lower()
        if any(k in alvo for k in SERVICOS_CALCADA):
            mantidas += 1
            escritor.writerow(row)
    return lidas, mantidas


def coletar_sp156(ds: str, chegada: Path, url_csv: str | None = None) -> dict:
    url = url_csv or _url_csv_mais_recente()
    destino = _destino(chegada, "sp156", ds, "csv")
    with requests.get(url, stream=True, timeout=TIMEOUT * 5) as r:
        r.raise_for_status()
        bruto = r.content  # arquivos semestrais cabem em memória (dezenas de MB)
    try:
        texto = bruto.decode("utf-8")
    except UnicodeDecodeError:
        texto = bruto.decode("latin-1")
    with destino.open("w", encoding="utf-8", newline="") as saida:
        lidas, mantidas = filtrar_calcadas(io.StringIO(texto, newline=""), saida)
    if mantidas == 0:
        destino.unlink(missing_ok=True)
        raise RuntimeError(f"SP156 sem linhas de calçada em {url}: confira o arquivo ou SP156_CSV_URL")
    return {"arquivo": str(destino), "origem": url, "lidas": lidas, "mantidas": mantidas}


# ---------------------------------------------------------------------------
# Fonte 3 - ocorrências do robô e da visão (API Calçada Segura)
# ---------------------------------------------------------------------------
def simular_ocorrencias(ds: str, n_min: int = 20, n_max: int = 60) -> list[dict]:
    """Gera ocorrências no formato do contrato enquanto a API não está no ar.

    Geração justificada (o PDF permite dados simulados documentados):
    - semente = data, então o mesmo ds gera sempre o mesmo lote (reprodutível);
    - ~5% dos registros vêm com defeito de propósito (fora de SP, tipo
      inválido, confiança > 1, id repetido) para exercitar as regras da Prata;
    - operador_id é um pseudônimo, que a Ouro transforma em hash (LGPD).
    """
    rnd = random.Random(ds)
    base = datetime.fromisoformat(ds).replace(hour=8)
    regs = []
    for _ in range(rnd.randint(n_min, n_max)):
        tipo = rnd.choice(TIPOS)
        reg = {
            "id": str(uuid.UUID(int=rnd.getrandbits(128))),
            "origem": rnd.choice(["robo", "visao"]),
            "tipo": tipo,
            "lat": round(rnd.uniform(-23.70, -23.45), 6),
            "lon": round(rnd.uniform(-46.78, -46.45), 6),
            "foto_url": None,
            "confianca": round(rnd.uniform(0.45, 0.99), 2),
            "inclinacao_pct": round(rnd.uniform(9, 16), 1) if tipo == "rampa_irregular" else round(rnd.uniform(0, 8), 1),
            "vibracao_rms": round(rnd.uniform(0.6, 1.5), 2) if tipo == "trepidacao" else round(rnd.uniform(0.05, 0.5), 2),
            "criado_em": (base + timedelta(minutes=rnd.randint(0, 600))).isoformat() + "-03:00",
            "status": "nova",
            "operador_id": f"operador-{rnd.randint(1, 4):02d}",
            "simulado": True,
        }
        reg["foto_url"] = f"https://exemplo.invalid/fotos/{reg['id']}.jpg" if reg["origem"] == "visao" else None
        defeito = rnd.random()
        if defeito < 0.02:
            reg["lat"], reg["lon"] = -22.90, -43.20          # fora de SP (Rio)
        elif defeito < 0.035:
            reg["tipo"] = "buraco??"                         # tipo inválido
        elif defeito < 0.05:
            reg["confianca"] = 1.7                           # fora de [0, 1]
        regs.append(reg)
    if len(regs) > 3:
        regs.append(dict(regs[0]))                           # duplicado proposital
    return regs


def coletar_ocorrencias(ds: str, chegada: Path, api_url: str | None = None, api_key: str | None = None) -> dict:
    if api_url:
        r = requests.get(
            f"{api_url.rstrip('/')}/ocorrencias",
            params={"data": ds},
            headers={"X-API-Key": api_key or ""},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        regs, origem = r.json(), "api"
    else:
        regs, origem = simular_ocorrencias(ds), "simulado"
    destino = _destino(chegada, "ocorrencias", ds, "json")
    destino.write_text(json.dumps(regs, ensure_ascii=False), encoding="utf-8")
    return {"arquivo": str(destino), "origem": origem, "registros": len(regs)}


# ---------------------------------------------------------------------------
# Verificação (requisito 03)
# ---------------------------------------------------------------------------
ESPERADOS = {"clima": "json", "sp156": "csv", "ocorrencias": "json"}


def conferir_chegada(ds: str, chegada: Path) -> dict:
    faltando, tamanhos = [], {}
    for fonte, ext in ESPERADOS.items():
        arq = Path(chegada) / fonte / f"{fonte}_{ds}.{ext}"
        if not arq.exists() or arq.stat().st_size == 0:
            faltando.append(str(arq))
        else:
            tamanhos[fonte] = arq.stat().st_size
    if faltando:
        raise FileNotFoundError(f"Arquivos não chegaram: {faltando}")
    return tamanhos

# Calçada Segura — componente Dados (Big Data Architecture & Data Integration)

GS FIAP 2026.2 · CitizenTech · Engenharia de Software 4ES

## Integrantes

| Nome completo | RM |
| --- | --- |
| (Enzo Luiz Goulart) | RM99666 |
| (João Pedro Cruz) | RM98650 |
| (Tiago Paulino) | RM551169 |
| (Victor Eid) | RM98668 |

## Ficha Técnica do MVP

| Campo | Valor |
| --- | --- |
| Produto | Calçada Segura |
| Cidadão e território | Pedestres de São Paulo, em especial idosos e pessoas com deficiência |
| Problema | Buracos, rampas fora da norma e barreiras nas calçadas não são fiscalizados |
| Proposta de valor | Robô e visão computacional inspecionam a calçada; o sistema prioriza e encaminha o conserto |
| Fontes de dados | SP156 (Prefeitura de SP), Open-Meteo, ocorrências do robô (API Calçada Segura) |
| URL do MVP | (preencher) |

## Perguntas que o MVP responde

1. Quais subprefeituras têm mais reclamações de calçada no SP156?
2. As reclamações e os problemas encontrados aumentam depois de dias de chuva?
3. Quanto tempo a Prefeitura leva para atender uma reclamação de calçada?
4. Que tipo de problema o robô encontra mais, e onde?
5. **(impacto)** Quantos problemas o robô achou em locais sem nenhuma reclamação no SP156?

## Arquitetura

```
Open-Meteo ─┐
SP156 ──────┼─► Airflow (gs_coleta, diário) ─► chegada/ ─► upload em lotes ─► Volume Databricks
API robô ───┘                                                                     │
                                         Bronze (Auto Loader) ─► Prata (MERGE + qualidade) ─► Ouro (fato + dimensões)
                                                                                                   │
                                                                     consultas exportadas ─► GET /indicadores ─► dashboard
```

O Databricks Free Edition não conecta no Airflow local: os arquivos de `chegada/` sobem para o Volume pela interface, em lotes de dias diferentes.

## Fontes

| Fonte | Origem | Formato | Frequência de coleta |
| --- | --- | --- | --- |
| Clima | API Open-Meteo (`/v1/forecast`, sem chave) | JSON | diária |
| SP156 | Portal de Dados Abertos da Prefeitura de SP, pacote `dados-do-sp156` (CKAN) | CSV | diária (o portal atualiza por período) |
| Ocorrências | API Calçada Segura `GET /ocorrencias` | JSON | diária |

**Filtro na coleta do SP156:** o arquivo completo tem cerca de 200 assuntos. A coleta mantém só as solicitações cujo **Serviço** é sobre calçada: "Calçada pública - solicitar manutenção", "Calçada particular - denunciar calçada danificada ou inexistente", "Acessibilidade - solicitar avaliação de obstáculo na calçada" e "Sugerir construção de calçadas". No 2º trimestre de 2026 isso deu 4.273 de 463.686 linhas. Pedidos de poda de árvore "em calçadas" ficam de fora de propósito.

**Ocorrências simuladas (até a API entrar no ar):** sem a Variable `API_URL`, o DAG gera ocorrências no formato do contrato da equipe. A semente é a data, então o mesmo dia gera sempre o mesmo lote. Cerca de 5% dos registros têm defeito de propósito (fora de SP, tipo inválido, confiança > 1) e há um registro duplicado, para exercitar as regras de qualidade da Prata. Todo registro simulado traz `"simulado": true`.

## Como rodar o Airflow

Pré-requisito: Docker Desktop.

```bash
docker compose up -d
# esperar ~1 min e abrir http://localhost:8080
```

1. Em **Admin → Variables**, criar (todas opcionais):
   - `SP156_CSV_URL`: link direto do CSV mais recente, se a busca automática no CKAN falhar
   - `API_URL` e `API_KEY`: quando a API do MVP estiver publicada
   - `NOTIFY_WEBHOOK`: webhook do Discord/Slack para avisos de falha e sucesso
2. Ativar o DAG `gs_coleta` e clicar em **Trigger** para a primeira coleta.
3. Conferir os arquivos em `chegada/clima`, `chegada/sp156` e `chegada/ocorrencias`.

Para pausar use `docker compose stop` (não `down`, que apaga o histórico usado nos prints de Graph e Grid).

## DAG `gs_coleta`

```
inicio >> [coletar_clima, coletar_sp156, coletar_ocorrencias] >> conferir_chegada >> fim
```

- agendado `@daily`, 2 retentativas com 5 min de espera
- 3 coletas em paralelo e 1 tarefa que confere se os 3 arquivos chegaram
- um arquivo por coleta, com a data no nome: `chegada/<fonte>/<fonte>_<AAAA-MM-DD>.<ext>`
- chaves e URLs em Variables, nunca no código
- notificação de falha (qualquer tarefa) e de sucesso (lote completo)

## Estrutura

```
dags/            DAG e funções de coleta (dags/coletas/fontes.py)
databricks/      01_bronze, 02_prata, 03_ouro (notebooks Python) e 04_consultas.sql
amostras/        um pouco de cada fonte
chegada/         pasta de pouso (fora do Git)
docs/            prints e relatório
```

## LGPD

- Nenhuma chave, senha ou dado pessoal real no repositório.
- `operador_id` vira hash SHA-256 com sal na Ouro; coordenadas são arredondadas na Ouro.
- Fotos chegam já com rostos e placas borrados pelo componente de Visão.

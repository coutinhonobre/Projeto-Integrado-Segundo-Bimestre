"""DAG dedicada a provisionar o dashboard do Metabase (secao 4.7 do PDF) sem
passos manuais no host.

Diferente da cadeia bronze -> silver -> gold (`wisdm_silver/assets.py`), esta
DAG NAO usa Asset scheduling: o dashboard depende de duas coisas produzidas
por caminhos diferentes - os modelos dbt da Gold (`gold_activity_benchmark`,
`gold_subject_activity_summary`, via DAG `wisdm_silver_to_gold`) E os
resultados do modelo de ML (`ml_model_comparison`,
`ml_model_classification_report`, publicados manualmente pela ultima celula
do notebook `mlp_activity_classification_academico.ipynb`, que nao roda
dentro do Airflow). Como um dos dois produtores nao e rastreado pelo
scheduler, nao da pra expressar isso como dependencia de dados automatica -
`schedule=None` (disparo manual, via UI/CLI/API do Airflow) depois que os
dois passos anteriores tiverem terminado.

Sequencia:
    1. verify_gold_ready: confere no Glue que as 4 tabelas existem antes de
       mexer no Metabase (mesmo espirito de `verify_bronze_ready` /
       `verify_silver_ready` nas outras DAGs da medalhao).
    2. sync_metabase_schema: forca o Metabase a re-sincronizar o schema da
       conexao Athena (`WISDM Gold (Athena)`, criada uma vez pelo container
       `metabase-init` no primeiro boot) - sem isso, tabelas novas na Gold
       nao aparecem pro Metabase ate o proximo sync automatico (que pode
       demorar).
    3. build_dashboard: chama `metabase/build_dashboard.py` (montado
       read-only em /opt/airflow/metabase pelo docker-compose) via
       subprocess, reaproveitando o script existente em vez de duplicar a
       logica de criacao dos cards/dashboard. Idempotente, como o proprio
       script ja documenta.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import dag, task

DATABASE_NAME = "WISDM Gold (Athena)"
REQUIRED_GOLD_TABLES = {
    "gold_activity_benchmark",
    "gold_subject_activity_summary",
    "ml_model_comparison",
    "ml_model_classification_report",
}


@task
def verify_gold_ready() -> None:
    import boto3

    database = os.environ.get("GOLD_GLUE_DATABASE_NAME", "projeto_integrado_wisdm_gold")
    glue = boto3.client("glue", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))

    existing = {t["Name"] for t in glue.get_tables(DatabaseName=database)["TableList"]}
    missing = REQUIRED_GOLD_TABLES - existing
    if missing:
        raise RuntimeError(
            f"Gold incompleta: faltam as tabelas {sorted(missing)} em `{database}`. "
            "Rode a DAG wisdm_silver_to_gold ate o fim e, se faltar "
            "ml_model_comparison/ml_model_classification_report, rode o notebook "
            "mlp_activity_classification_academico.ipynb de ponta a ponta (a ultima "
            "celula publica essas 2 tabelas via ml/export_model_results.py) antes de "
            "disparar esta DAG de novo."
        )
    print(f"Gold ok: as {len(REQUIRED_GOLD_TABLES)} tabelas esperadas existem em `{database}`.")


@task
def sync_metabase_schema() -> None:
    import json
    import time
    import urllib.error
    import urllib.request

    mb_url = os.environ.get("MB_URL", "http://metabase:3000").rstrip("/")

    def request(method, path, body=None, session=None):
        url = f"{mb_url}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if session:
            req.add_header("X-Metabase-Session", session)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except json.JSONDecodeError:
                return e.code, raw.decode(errors="replace")

    status, resp = request(
        "POST",
        "/api/session",
        {"username": os.environ["METABASE_ADMIN_EMAIL"], "password": os.environ["METABASE_ADMIN_PASSWORD"]},
    )
    if status >= 300:
        raise RuntimeError(f"Login no Metabase falhou ({status}): {resp}")
    session = resp["id"]

    status, resp = request("GET", "/api/database", session=session)
    rows = resp.get("data", resp) if isinstance(resp, dict) else resp
    matches = [r["id"] for r in rows if r["name"] == DATABASE_NAME]
    if not matches:
        raise RuntimeError(
            f"Database '{DATABASE_NAME}' nao encontrado no Metabase - o container "
            "metabase-init rodou com sucesso no primeiro boot?"
        )
    database_id = matches[0]

    status, resp = request("POST", f"/api/database/{database_id}/sync_schema", session=session)
    if status >= 300:
        raise RuntimeError(f"Falha ao disparar sync_schema ({status}): {resp}")
    print(f"Sync do schema disparado para '{DATABASE_NAME}' (id={database_id}).")

    # sync_schema e assincrono do lado do Metabase - espera as tabelas
    # aparecerem antes de deixar o build_dashboard tentar usa-las.
    timeout_seconds, poll_seconds, waited = 180, 5, 0
    while waited < timeout_seconds:
        status, resp = request("GET", f"/api/database/{database_id}?include=tables", session=session)
        table_names = {t["name"] for t in resp.get("tables", [])}
        if REQUIRED_GOLD_TABLES <= table_names:
            print("Todas as tabelas Gold ja apareceram no Metabase.")
            return
        time.sleep(poll_seconds)
        waited += poll_seconds

    raise RuntimeError(
        f"Timeout esperando o Metabase sincronizar as tabelas {sorted(REQUIRED_GOLD_TABLES)} "
        f"(id={database_id}) apos {timeout_seconds}s."
    )


@task
def build_dashboard() -> None:
    import subprocess

    subprocess.run(["python3", "/opt/airflow/metabase/build_dashboard.py"], check=True)


@dag(
    dag_id="wisdm_metabase_dashboard",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["metabase", "gold", "dashboard"],
)
def wisdm_metabase_dashboard():
    gold_ready = verify_gold_ready()
    schema_synced = sync_metabase_schema()
    dashboard_built = build_dashboard()
    gold_ready >> schema_synced >> dashboard_built


wisdm_metabase_dashboard()

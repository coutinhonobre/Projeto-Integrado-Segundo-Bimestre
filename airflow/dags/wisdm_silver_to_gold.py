"""DAG dedicada a camada Ouro (Gold) do projeto WISDM.

Responsabilidade unica: materializar os modelos dbt em `dbt/models/gold`
(agregados de negocio sobre os marts da Silver - `dim_subject` e
`fct_wisdm_windows`) e rodar os testes de dados correspondentes.

Orquestracao: em vez de agendamento por horario (cron) ou um
`TriggerDagRunOperator` explicito acoplado ao `dag_id` da Silver, esta DAG usa
`schedule=[SILVER_DBT_ASSET]` (Airflow Assets, Airflow 3). O scheduler dispara
esta DAG automaticamente assim que a task `run_dbt` da DAG
`wisdm_bronze_to_silver` (que declara o mesmo Asset como `outlets`) terminar
com sucesso - uma dependencia de DADOS, nao de tempo, entre as duas camadas.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import dag, task

from wisdm_silver.assets import SILVER_DBT_ASSET

SILVER_TABLE_NAME = "wisdm_features"


@task
def verify_silver_ready() -> None:
    """Verifica que a Silver registrou os 51 sujeitos esperados no Glue
    antes de gastar tempo/dinheiro rodando os modelos gold em cima dela.

    O Asset `SILVER_DBT_ASSET` (via `schedule=[...]` no `@dag` abaixo) so
    garante que a task `run_dbt` da Silver TERMINOU COM SUCESSO - nao que ela
    rodou sobre um conjunto completo de sujeitos. Checar as particoes
    registradas na tabela externa (equivalente ao que a Silver de fato
    materializou) e mais rapido e barato do que rodar uma query Athena so
    pra contar linhas.
    """
    import boto3

    from wisdm_silver.schema import EXPECTED_SUBJECTS

    database = os.environ["GLUE_DATABASE_NAME"]
    glue = boto3.client("glue", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))

    partitions = glue.get_partitions(DatabaseName=database, TableName=SILVER_TABLE_NAME)
    registered_subjects = {p["Values"][0] for p in partitions["Partitions"]}

    missing = sorted(EXPECTED_SUBJECTS - registered_subjects)
    if missing:
        raise RuntimeError(
            f"Silver desatualizada/incompleta: faltam os sujeitos {missing} "
            f"nas particoes de `{database}`.`{SILVER_TABLE_NAME}`. Rode a DAG "
            "wisdm_bronze_to_silver de ponta a ponta ate ela terminar com "
            "sucesso para todos os 51 sujeitos antes da Gold prosseguir."
        )

    print(f"Silver ok: os {len(EXPECTED_SUBJECTS)} sujeitos esperados estao registrados.")


@task
def run_dbt_gold() -> None:
    """Roda `dbt run` + `dbt test` restritos a camada Gold.

    `--select gold+` inclui a propria camada gold e qualquer coisa rio abaixo
    dela. Os marts (Silver) de que a Gold depende via `ref()` ja foram
    materializados pela DAG `wisdm_bronze_to_silver` antes deste Asset
    disparar esta DAG - nao precisam ser reconstruidos aqui.
    """
    import subprocess

    project_dir = "/opt/airflow/dbt"
    profiles_dir = "/opt/airflow/dbt"

    for cmd in (["dbt", "run"], ["dbt", "test"]):
        full_cmd = cmd + [
            "--project-dir", project_dir,
            "--profiles-dir", profiles_dir,
            "--select", "gold+",
        ]
        print(f"Executando: {' '.join(full_cmd)}")
        subprocess.run(full_cmd, check=True)


@dag(
    dag_id="wisdm_silver_to_gold",
    start_date=datetime(2026, 1, 1),
    schedule=[SILVER_DBT_ASSET],
    catchup=False,
    tags=["gold", "athena", "glue", "dbt", "wisdm"],
)
def wisdm_silver_to_gold():
    silver_ready = verify_silver_ready()
    gold_ready = run_dbt_gold()
    silver_ready >> gold_ready


wisdm_silver_to_gold()

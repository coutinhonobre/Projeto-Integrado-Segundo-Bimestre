from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import dag, task

from wisdm_silver.assets import BRONZE_S3_ASSET, SILVER_DBT_ASSET

SILVER_TABLE_NAME = "wisdm_features"


@task
def verify_bronze_ready() -> None:
    """Verifica que a camada Bronze (S3 raw) tem os 4 arquivos brutos
    (phone_accel, phone_gyro, watch_accel, watch_gyro) de todos os sujeitos
    esperados antes de comecar a extrair qualquer coisa.

    Mesma filosofia do projeto WESAD anterior: um bronze parcial (bucket
    resetado no meio de um re-upload, por exemplo) nao deve deixar a Silver
    seguir em frente processando so um subconjunto dos sujeitos.
    """
    import boto3

    from wisdm_silver.schema import EXPECTED_SUBJECTS, RAW_FILE_KINDS, raw_filename

    bucket = os.environ["S3_BUCKET_NAME"]
    raw_prefix = os.environ.get("S3_RAW_PREFIX", "raw/wisdm").strip("/")

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    found_files = {
        os.path.basename(obj["Key"])
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{raw_prefix}/")
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".txt")
    }

    missing: dict[str, list[str]] = {}
    for subject_id in EXPECTED_SUBJECTS:
        expected = {raw_filename(subject_id, device, sensor) for device, sensor in RAW_FILE_KINDS}
        subject_missing = expected - found_files
        if subject_missing:
            missing[subject_id] = sorted(subject_missing)

    if missing:
        sample = dict(sorted(missing.items())[:5])
        raise RuntimeError(
            f"Bronze incompleta em s3://{bucket}/{raw_prefix}/: {len(missing)} sujeito(s) "
            f"com arquivo(s) faltando (amostra: {sample}). Rode (ou re-rode) a DAG "
            "kaggle_wisdm_to_s3 ate ela terminar com sucesso antes de rodar esta DAG."
        )

    print(f"Bronze ok: os {len(EXPECTED_SUBJECTS)} sujeitos esperados tem os 4 arquivos brutos.")


@task
def extract_features_to_silver() -> list[str]:
    """Le watch_accel/watch_gyro de cada sujeito na camada Bronze (S3),
    sincroniza e janela os sinais, e grava Parquet particionado por sujeito
    na camada Silver (S3).

    So processa os arquivos do device `watch` (decisao de escopo da secao 2
    do SDD) - `phone_accel`/`phone_gyro` ficam na Bronze sem uso por ora.

    Retorna a lista de subject_ids processados com sucesso, usada pela task
    seguinte para registrar as particoes no Glue/Athena.
    """
    import boto3

    from wisdm_silver.feature_extraction import extract_features_for_subject, load_subject_files
    from wisdm_silver.schema import EXPECTED_SUBJECTS, raw_filename

    bucket = os.environ["S3_BUCKET_NAME"]
    raw_prefix = os.environ.get("S3_RAW_PREFIX", "raw/wisdm").strip("/")
    silver_prefix = os.environ.get("S3_SILVER_PREFIX", "silver/wisdm_features").strip("/")

    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    key_by_basename = {
        os.path.basename(obj["Key"]): obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{raw_prefix}/")
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".txt")
    }

    if not key_by_basename:
        raise RuntimeError(
            f"Nenhum arquivo .txt encontrado em s3://{bucket}/{raw_prefix}/ - "
            "rode a DAG kaggle_wisdm_to_s3 (bronze) antes desta."
        )

    processed_subjects: list[str] = []
    for subject_id in sorted(EXPECTED_SUBJECTS):
        accel_key = key_by_basename.get(raw_filename(subject_id, "watch", "accel"))
        gyro_key = key_by_basename.get(raw_filename(subject_id, "watch", "gyro"))
        if accel_key is None or gyro_key is None:
            print(f"Sujeito {subject_id}: arquivos watch ausentes na Bronze, pulando.")
            continue

        print(f"Processando sujeito {subject_id}: s3://{bucket}/{accel_key}, s3://{bucket}/{gyro_key}")
        accel_bytes = s3.get_object(Bucket=bucket, Key=accel_key)["Body"].read()
        gyro_bytes = s3.get_object(Bucket=bucket, Key=gyro_key)["Body"].read()

        accel_df, gyro_df = load_subject_files(accel_bytes, gyro_bytes)
        features = extract_features_for_subject(accel_df, gyro_df)
        if features.empty:
            print(f"Sujeito {subject_id}: nenhuma janela valida, pulando.")
            continue

        local_path = f"/tmp/{subject_id}_features.parquet"
        features.to_parquet(local_path, index=False)

        silver_key = f"{silver_prefix}/subject={subject_id}/part-0000.parquet"
        s3.upload_file(local_path, bucket, silver_key)
        os.remove(local_path)

        print(f"Sujeito {subject_id}: {len(features)} janelas -> s3://{bucket}/{silver_key}")
        processed_subjects.append(subject_id)

    if not processed_subjects:
        raise RuntimeError("Nenhum sujeito gerou janelas validas para a camada Silver.")

    missing = sorted(EXPECTED_SUBJECTS - set(processed_subjects))
    if missing:
        raise RuntimeError(
            f"Extracao incompleta: {len(processed_subjects)} de "
            f"{len(EXPECTED_SUBJECTS)} sujeitos esperados geraram janelas "
            f"validas. Faltam: {missing}. Nao vou registrar particoes "
            "parciais no Glue silenciosamente - investigue esses sujeitos "
            "(arquivo corrompido? threshold de pureza descartou tudo?) antes de "
            "prosseguir."
        )

    return processed_subjects


@task
def ensure_glue_database() -> None:
    import boto3
    from botocore.exceptions import ClientError

    database = os.environ["GLUE_DATABASE_NAME"]
    glue = boto3.client("glue", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))

    try:
        glue.get_database(Name=database)
        print(f"Glue database ja existe: {database}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        print(f"Glue database nao existe, criando: {database}")
        glue.create_database(DatabaseInput={"Name": database})
        print(f"Glue database criado: {database}")


@task
def register_athena_table(subject_ids: list[str]) -> None:
    """Cria (se necessario) a tabela externa `wisdm_features` no Glue Data
    Catalog e registra uma particao por sujeito processado, apontando para o
    respectivo diretorio `subject=<id>/` na Silver do S3.
    """
    import boto3

    from wisdm_silver.athena_utils import run_athena_query
    from wisdm_silver.schema import FEATURE_COLUMNS

    bucket = os.environ["S3_BUCKET_NAME"]
    silver_prefix = os.environ.get("S3_SILVER_PREFIX", "silver/wisdm_features").strip("/")
    results_prefix = os.environ.get("ATHENA_RESULTS_PREFIX", "athena-results").strip("/")
    database = os.environ["GLUE_DATABASE_NAME"]
    workgroup = os.environ.get("ATHENA_WORKGROUP", "primary")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    table_location = f"s3://{bucket}/{silver_prefix}/"
    output_location = f"s3://{bucket}/{results_prefix}/"

    athena = boto3.client("athena", region_name=region)

    columns_ddl = ",\n  ".join(f"`{name}` {athena_type}" for name, athena_type in FEATURE_COLUMNS)
    create_table_sql = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{database}`.`{SILVER_TABLE_NAME}` (
  {columns_ddl}
)
PARTITIONED BY (`subject` string)
STORED AS PARQUET
LOCATION '{table_location}'
""".strip()

    run_athena_query(athena, create_table_sql, database, output_location, workgroup)
    print(f"Tabela registrada: {database}.{SILVER_TABLE_NAME} -> {table_location}")

    for subject_id in subject_ids:
        partition_location = f"{table_location}subject={subject_id}/"
        add_partition_sql = (
            f"ALTER TABLE `{database}`.`{SILVER_TABLE_NAME}` "
            f"ADD IF NOT EXISTS PARTITION (`subject`='{subject_id}') "
            f"LOCATION '{partition_location}'"
        )
        run_athena_query(athena, add_partition_sql, database, output_location, workgroup)
        print(f"Particao registrada: subject={subject_id}")


@task(outlets=[SILVER_DBT_ASSET])
def run_dbt() -> None:
    """Materializa apenas a camada Silver do dbt (staging + marts).

    O `--select staging marts` e deliberado: a camada Gold (dbt/models/gold)
    tambem mora neste mesmo projeto dbt e depende dos marts via `ref()`, mas
    e responsabilidade de uma DAG separada (`wisdm_silver_to_gold`), disparada
    automaticamente pelo `outlets=[SILVER_DBT_ASSET]` acima assim que esta
    task terminar com sucesso.
    """
    import subprocess

    project_dir = "/opt/airflow/dbt"
    profiles_dir = "/opt/airflow/dbt"

    for cmd in (["dbt", "run"], ["dbt", "test"]):
        full_cmd = cmd + [
            "--project-dir", project_dir,
            "--profiles-dir", profiles_dir,
            "--select", "staging", "marts",
        ]
        print(f"Executando: {' '.join(full_cmd)}")
        subprocess.run(full_cmd, check=True)


@dag(
    dag_id="wisdm_bronze_to_silver",
    start_date=datetime(2026, 1, 1),
    schedule=[BRONZE_S3_ASSET],
    catchup=False,
    tags=["silver", "athena", "glue", "dbt", "wisdm"],
)
def wisdm_bronze_to_silver():
    bronze_ready = verify_bronze_ready()
    subjects = extract_features_to_silver()
    db_ready = ensure_glue_database()
    table_ready = register_athena_table(subjects)
    dbt_ready = run_dbt()

    bronze_ready >> subjects
    db_ready >> table_ready
    table_ready >> dbt_ready


wisdm_bronze_to_silver()

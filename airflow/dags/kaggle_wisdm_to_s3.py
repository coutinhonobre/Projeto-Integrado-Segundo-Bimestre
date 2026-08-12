import os
from datetime import datetime

from airflow.sdk import dag, task

from wisdm_silver.assets import BRONZE_S3_ASSET

KAGGLE_DATASET = "mashlyn/smartphone-and-smartwatch-activity-and-biometrics"


@task
def ensure_bucket() -> None:
    import boto3
    from botocore.exceptions import ClientError

    bucket = os.environ["S3_BUCKET_NAME"]
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    s3 = boto3.client("s3")

    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket ja existe: {bucket}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise
        print(f"Bucket nao existe, criando: {bucket}")
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"Bucket criado: {bucket}")


@task
def download_dataset() -> str:
    import kagglehub

    path = kagglehub.dataset_download(KAGGLE_DATASET)
    print(f"Dataset baixado em: {path}")
    return path


@task
def verify_download_complete(dataset_path: str) -> str:
    """Verifica que o download do Kaggle trouxe os 4 arquivos brutos
    (phone_accel, phone_gyro, watch_accel, watch_gyro) de todos os sujeitos
    esperados antes de subir qualquer coisa pro S3.

    Sem essa checagem, um download parcial/interrompido (rede, timeout do
    kagglehub) seria enviado pro S3 do mesmo jeito e as camadas seguintes
    (silver, gold) processariam silenciosamente um subconjunto incompleto de
    sujeitos - foi exatamente esse silencio que causou um problema real no
    projeto WESAD anterior, com a Silver avancando com so 3 dos 15 sujeitos.

    Confere os 4 tipos de arquivo (nao so watch_accel/watch_gyro, que e o
    que a Silver de fato processa) porque o kagglehub baixa o dataset
    inteiro de qualquer forma - completude da Bronze deve refletir o que foi
    realmente baixado/subido, independente do que a Silver escolhe consumir.
    """
    import os

    from wisdm_silver.schema import EXPECTED_SUBJECTS, RAW_FILE_KINDS, raw_filename

    found_files = {
        filename
        for _root, _dirs, files in os.walk(dataset_path)
        for filename in files
        if filename.endswith(".txt")
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
            f"Download do Kaggle incompleto: {len(missing)} sujeito(s) com arquivo(s) "
            f"faltando em {dataset_path} (amostra: {sample}). Nao vou subir pro S3 um "
            "dataset parcial - rode a DAG de novo (kagglehub costuma cachear o que ja "
            "baixou com sucesso)."
        )

    print(f"Download completo: os {len(EXPECTED_SUBJECTS)} sujeitos esperados tem os 4 arquivos brutos.")
    return dataset_path


@task(outlets=[BRONZE_S3_ASSET])
def upload_to_s3(dataset_path: str) -> None:
    """Sobe o dataset baixado (ja verificado completo) pro S3.

    Declara `outlets=[BRONZE_S3_ASSET]` pra disparar automaticamente a DAG
    `wisdm_bronze_to_silver` assim que terminar com sucesso - ver
    `wisdm_silver/assets.py` pro encadeamento completo bronze -> silver ->
    gold.
    """
    import boto3

    bucket = os.environ["S3_BUCKET_NAME"]
    prefix = os.environ.get("S3_RAW_PREFIX", "raw/wisdm").strip("/")
    s3 = boto3.client("s3")

    for root, _, files in os.walk(dataset_path):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, dataset_path)
            key = f"{prefix}/{relative_path}"
            s3.upload_file(local_path, bucket, key)
            print(f"Enviado: {local_path} -> s3://{bucket}/{key}")


@dag(
    dag_id="kaggle_wisdm_to_s3",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["raw", "s3", "kaggle"],
)
def kaggle_wisdm_to_s3():
    bucket_ready = ensure_bucket()
    dataset_path = download_dataset()
    verified_path = verify_download_complete(dataset_path)
    upload_task = upload_to_s3(verified_path)
    bucket_ready >> upload_task


kaggle_wisdm_to_s3()

"""Publica os resultados do modelo (notebook `mlp_activity_classification_academico.ipynb`)
na camada Gold, como 2 tabelas Athena/Glue consumiveis pelo dashboard do
Metabase (secao 4.7 do PDF: "visualizacao dos resultados do modelo").

Uso normal: chamado automaticamente pela ultima celula do notebook, com os
numeros calculados na hora (`publish(comparison_rows=..., classification_rows=...)`).

Uso standalone (`python3 export_model_results.py`, sem argumentos): republica
os ultimos numeros conhecidos (constantes abaixo, extraidas do notebook rodado
com seed=42) - util pra recriar as tabelas Gold sem precisar reabrir o Jupyter.

Requer: boto3 e as mesmas credenciais AWS do .env do projeto no ambiente.
"""

import csv
import os
import sys
from pathlib import Path

ML_DIR = Path(__file__).parent
sys.path.insert(0, str(ML_DIR.parent / "airflow" / "dags"))
from wisdm_silver.athena_utils import run_athena_query  # noqa: E402

ACTIVITY_NAMES = {
    "A": "walking", "B": "jogging", "C": "stairs", "D": "sitting", "E": "standing",
    "F": "typing", "G": "brushing_teeth", "H": "eating_soup", "I": "eating_chips",
    "J": "eating_pasta", "K": "drinking", "L": "eating_sandwich", "M": "kicking_ball",
    "O": "playing_catch", "P": "dribbling_basketball", "Q": "writing", "R": "clapping",
    "S": "folding_clothes",
}

# Ultimos numeros conhecidos (notebook rodado com seed=42) - usados so quando
# este script roda standalone, sem o notebook ter passado numeros frescos.
DEFAULT_MODEL_COMPARISON = [
    {"modelo": "baseline_majoritario", "accuracy": 0.056038, "precision_macro": 0.003113,
     "recall_macro": 0.055556, "f1_macro": 0.005896},
    {"modelo": "mlp_hardcode", "accuracy": 0.675612, "precision_macro": 0.672013,
     "recall_macro": 0.676213, "f1_macro": 0.667739},
    {"modelo": "mlp_sklearn", "accuracy": 0.677585, "precision_macro": 0.675043,
     "recall_macro": 0.678542, "f1_macro": 0.670341},
]

DEFAULT_CLASSIFICATION_REPORT = [
    ("A", 0.75, 0.76, 0.75, 139), ("B", 0.89, 0.97, 0.93, 139), ("C", 0.73, 0.73, 0.73, 139),
    ("D", 0.60, 0.70, 0.65, 144), ("E", 0.59, 0.56, 0.57, 144), ("F", 0.59, 0.74, 0.66, 142),
    ("G", 0.59, 0.60, 0.59, 140), ("H", 0.64, 0.79, 0.71, 141), ("I", 0.59, 0.61, 0.60, 140),
    ("J", 0.62, 0.54, 0.58, 154), ("K", 0.53, 0.65, 0.58, 142), ("L", 0.27, 0.14, 0.18, 139),
    ("M", 0.68, 0.50, 0.57, 135), ("O", 0.64, 0.87, 0.74, 131), ("P", 0.82, 0.54, 0.66, 138),
    ("Q", 0.79, 0.75, 0.77, 142), ("R", 0.95, 0.89, 0.92, 142), ("S", 0.82, 0.83, 0.83, 143),
]

OUT_DIR = ML_DIR / "data" / "model_results"


def write_csvs(comparison_rows, classification_rows):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    comparison_path = OUT_DIR / "ml_model_comparison.csv"
    with comparison_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["modelo", "accuracy", "precision_macro", "recall_macro", "f1_macro"])
        writer.writeheader()
        writer.writerows(comparison_rows)

    report_path = OUT_DIR / "ml_model_classification_report.csv"
    with report_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["activity_label", "activity_name", "precision", "recall", "f1_score", "support"])
        for label, precision, recall, f1, support in classification_rows:
            writer.writerow([label, ACTIVITY_NAMES[label], precision, recall, f1, support])

    return comparison_path, report_path


def publish(comparison_rows=None, classification_rows=None):
    import boto3

    comparison_rows = comparison_rows or DEFAULT_MODEL_COMPARISON
    classification_rows = classification_rows or DEFAULT_CLASSIFICATION_REPORT

    bucket = os.environ["S3_BUCKET_NAME"]
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    database = os.environ.get("GOLD_GLUE_DATABASE_NAME", "projeto_integrado_wisdm_gold")
    workgroup = os.environ.get("ATHENA_WORKGROUP", "primary")
    results_prefix = os.environ.get("ATHENA_RESULTS_PREFIX", "athena-results").strip("/")
    output_location = f"s3://{bucket}/{results_prefix}/"

    comparison_path, report_path = write_csvs(comparison_rows, classification_rows)

    comparison_prefix = "gold/ml_model_comparison"
    report_prefix = "gold/ml_model_classification_report"

    s3 = boto3.client("s3", region_name=region)
    s3.upload_file(str(comparison_path), bucket, f"{comparison_prefix}/ml_model_comparison.csv")
    s3.upload_file(str(report_path), bucket, f"{report_prefix}/ml_model_classification_report.csv")
    print(f"CSVs enviados para s3://{bucket}/gold/...")

    athena = boto3.client("athena", region_name=region)

    run_athena_query(
        athena,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS `{database}`.`ml_model_comparison` (
          `modelo` string,
          `accuracy` double,
          `precision_macro` double,
          `recall_macro` double,
          `f1_macro` double
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION 's3://{bucket}/{comparison_prefix}/'
        TBLPROPERTIES ('skip.header.line.count'='1')
        """,
        database, output_location, workgroup,
    )
    print(f"Tabela registrada: {database}.ml_model_comparison")

    run_athena_query(
        athena,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS `{database}`.`ml_model_classification_report` (
          `activity_label` string,
          `activity_name` string,
          `precision` double,
          `recall` double,
          `f1_score` double,
          `support` int
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION 's3://{bucket}/{report_prefix}/'
        TBLPROPERTIES ('skip.header.line.count'='1')
        """,
        database, output_location, workgroup,
    )
    print(f"Tabela registrada: {database}.ml_model_classification_report")


if __name__ == "__main__":
    publish()

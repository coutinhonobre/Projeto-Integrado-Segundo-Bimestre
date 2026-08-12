"""Esquema canonico da tabela Silver `wisdm_features`.

Fonte unica de verdade usada tanto por `feature_extraction.py` (colunas do
DataFrame/Parquet gerado por sujeito) quanto pela DAG `wisdm_bronze_to_silver`
(DDL do `CREATE EXTERNAL TABLE` no Glue Data Catalog/Athena). Manter as duas
pontas lendo da mesma lista evita que o Parquet gravado no S3 e a tabela
registrada no Glue fiquem com colunas fora de sincronia.

`subject` (ver `PARTITION_COLUMN`) e a coluna de particionamento Hive: fica
embutida no caminho `s3://.../subject=1600/...`, nunca dentro dos arquivos
Parquet.
"""

from __future__ import annotations

FEATURE_COLUMNS: list[tuple[str, str]] = [
    ("window_id", "bigint"),
    ("window_start_sample", "bigint"),
    ("n_samples", "bigint"),
    ("activity_label", "string"),
    ("label_purity", "double"),
]

_STAT_SIGNALS = ["accel_x", "accel_y", "accel_z", "gyro_x", "gyro_y", "gyro_z"]
_STATS = ["mean", "std", "min", "max"]

for _signal in _STAT_SIGNALS:
    for _stat in _STATS:
        FEATURE_COLUMNS.append((f"{_signal}_{_stat}", "double"))

PARTITION_COLUMN: tuple[str, str] = ("subject", "string")

# As 18 atividades do protocolo WISDM (codigo de uma letra - "N" nao e usado).
ACTIVITY_LABELS: dict[str, str] = {
    "A": "walking",
    "B": "jogging",
    "C": "stairs",
    "D": "sitting",
    "E": "standing",
    "F": "typing",
    "G": "brushing_teeth",
    "H": "eating_soup",
    "I": "eating_chips",
    "J": "eating_pasta",
    "K": "drinking",
    "L": "eating_sandwich",
    "M": "kicking_ball",
    "O": "playing_catch",
    "P": "dribbling_basketball",
    "Q": "writing",
    "R": "clapping",
    "S": "folding_clothes",
}

# Conjunto canonico dos 51 sujeitos do WISDM (IDs 1600-1650, nao "S1".."S51").
# Fonte unica de verdade usada pelas 3 DAGs da medalhao (bronze, silver, gold)
# para verificar se a camada anterior esta completa antes de prosseguir, em
# vez de assumir silenciosamente que "achei alguma coisa" significa "achei
# tudo" (mesma filosofia adotada no projeto WESAD anterior).
EXPECTED_SUBJECTS: frozenset[str] = frozenset(str(n) for n in range(1600, 1651))

# Os 4 arquivos brutos que o kagglehub baixa por sujeito (2 sensores x 2
# dispositivos). A Silver so processa watch_accel/watch_gyro (ver SDD secao 2),
# mas a Bronze verifica os 4 - o download traz o dataset inteiro de qualquer
# forma, entao um download/upload parcial deve ser detectado cedo mesmo para
# os arquivos que a Silver ainda nao consome.
RAW_FILE_KINDS: tuple[tuple[str, str], ...] = (
    ("phone", "accel"),
    ("phone", "gyro"),
    ("watch", "accel"),
    ("watch", "gyro"),
)


def raw_filename(subject_id: str, device: str, sensor: str) -> str:
    return f"data_{subject_id}_{sensor}_{device}.txt"

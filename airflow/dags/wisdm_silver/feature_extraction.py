"""Extracao de features (camada Silver) a partir dos sinais brutos do WISDM.

Le os arquivos de texto brutos watch_accel/watch_gyro de um sujeito (linhas
"subject-id,activity-code,timestamp,x,y,z;" a ~20Hz), sincroniza acelerometro
e giroscopio por timestamp (nao amostram exatamente nos mesmos instantes) e
janela o resultado em blocos fixos e DISJUNTOS de `window_seconds` (sem
sobreposicao - cada amostra sincronizada entra em uma unica janela),
calculando estatisticas simples (mean/std/min/max) por canal. Cada janela
"limpa" (ver `purity_threshold`) vira uma linha da tabela Silver
`wisdm_features`.

Sincronizacao accel/gyro: usa `pandas.merge_asof(direction="nearest")` com
tolerancia calculada dinamicamente como metade do delta mediano entre
timestamps consecutivos do proprio acelerometro, em vez de um valor fixo em
milissegundos - a documentacao publica do dataset diz apenas "Unix time" sem
confirmar a unidade exata (segundos, ms ou ns variam entre versoes/copias do
dataset), entao a tolerancia relativa se adapta sozinha e ainda captura a
intencao original do SDD (~metade do periodo nominal de amostragem a 20Hz).
Amostras de acelerometro sem par de giroscopio dentro da tolerancia sao
descartadas (contagem logada - risco ja sinalizado na secao 8 do SDD).

Janela fixa e disjunta sobre o stream sincronizado inteiro do sujeito (todas
as 18 atividades concatenadas, na ordem em que foram gravadas no arquivo) -
mesmo padrao usado no projeto WESAD anterior: nenhuma amostra reaproveitada
em mais de uma janela. Uma janela so vira linha se pelo menos
`purity_threshold` das amostras dentro dela pertencerem a mesma atividade
(descarta janelas de transicao entre atividades); isso tambem funciona como
defesa contra os problemas de qualidade conhecidos do WISDM bruto
(timestamps fora de ordem, rotulagem inconsistente - ver paper "rWISDM").

Limitacoes conhecidas (documentadas tambem no README):
- usa apenas o device `watch` (accel + gyro); o `phone` nao e processado
  nesta primeira versao da Silver (decisao de escopo do SDD, secao 2);
- os arquivos texto inteiros do sujeito sao carregados em memoria antes de
  sincronizar/janelar (bem menores que os .pkl do WESAD - ok para o escopo).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from wisdm_silver.schema import ACTIVITY_LABELS, FEATURE_COLUMNS

WATCH_FS_HZ = 20

_RAW_COLUMNS = ["subject", "activity_code", "timestamp", "x", "y", "z"]


def _parse_raw_text(text: str) -> pd.DataFrame:
    """Parseia um arquivo bruto do WISDM (linhas "s,a,t,x,y,z;").

    Cada linha termina em ';' (peculiaridade conhecida do dataset) - removida
    antes do split. Linhas malformadas (campos faltando, valor nao numerico -
    problema de qualidade ja documentado na literatura do WISDM) sao
    descartadas e contadas em vez de derrubar o parse inteiro.
    """
    rows: list[tuple[str, str, int, float, float, float]] = []
    malformed = 0
    for line in text.splitlines():
        line = line.strip().rstrip(";").strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 6:
            malformed += 1
            continue
        subject, activity_code, timestamp, x, y, z = parts
        try:
            rows.append((subject, activity_code, int(timestamp), float(x), float(y), float(z)))
        except ValueError:
            malformed += 1
            continue

    if malformed:
        print(f"Aviso: {malformed} linha(s) malformada(s) descartada(s) no parse.")

    return pd.DataFrame(rows, columns=_RAW_COLUMNS)


def load_subject_files(accel_bytes: bytes, gyro_bytes: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    accel_df = _parse_raw_text(accel_bytes.decode("utf-8", errors="replace"))
    gyro_df = _parse_raw_text(gyro_bytes.decode("utf-8", errors="replace"))
    return accel_df, gyro_df


def _sync_accel_gyro(accel_df: pd.DataFrame, gyro_df: pd.DataFrame) -> pd.DataFrame:
    """Casa cada amostra de acelerometro com a amostra de giroscopio mais
    proxima no tempo, dentro de uma tolerancia derivada dos proprios dados.

    Retorna colunas: activity_code, timestamp, accel_x/y/z, gyro_x/y/z.
    Amostras de accel sem par de gyro dentro da tolerancia sao descartadas.
    """
    accel_sorted = accel_df.sort_values("timestamp").reset_index(drop=True)
    gyro_sorted = gyro_df.sort_values("timestamp").reset_index(drop=True)

    deltas = accel_sorted["timestamp"].diff().dropna()
    median_delta = float(deltas.median()) if not deltas.empty else 0.0
    # merge_asof exige que a tolerancia tenha o mesmo dtype da chave (timestamp
    # e int64 - Unix time bruto do WISDM), entao arredonda para inteiro em vez
    # de usar o float direto.
    tolerance = int(round(max(median_delta / 2.0, 1.0)))

    merged = pd.merge_asof(
        accel_sorted,
        gyro_sorted[["timestamp", "x", "y", "z"]],
        on="timestamp",
        direction="nearest",
        tolerance=tolerance,
        suffixes=("_accel", "_gyro"),
    )

    total = len(merged)
    synced = merged.dropna(subset=["x_gyro", "y_gyro", "z_gyro"]).reset_index(drop=True)
    dropped = total - len(synced)
    if total:
        print(
            f"Sincronizacao accel/gyro: {dropped}/{total} amostras descartadas "
            f"(sem par de giroscopio dentro de {tolerance:.1f} unidades de tolerancia)."
        )

    return synced.rename(
        columns={
            "x_accel": "accel_x", "y_accel": "accel_y", "z_accel": "accel_z",
            "x_gyro": "gyro_x", "y_gyro": "gyro_y", "z_gyro": "gyro_z",
        }
    )[["activity_code", "timestamp", "accel_x", "accel_y", "accel_z", "gyro_x", "gyro_y", "gyro_z"]]


def _window_stats(values: np.ndarray) -> tuple[float, float, float, float]:
    return (
        float(np.mean(values)),
        float(np.std(values)),
        float(np.min(values)),
        float(np.max(values)),
    )


def extract_features_for_subject(
    accel_df: pd.DataFrame,
    gyro_df: pd.DataFrame,
    window_seconds: int = 10,
    purity_threshold: float = 0.9,
    fs: int = WATCH_FS_HZ,
) -> pd.DataFrame:
    """Sincroniza, janela (fixo/disjunto) e extrai atributos do watch de um
    sujeito, cobrindo todas as atividades gravadas no arquivo.

    Janela fixa de `window_seconds` sobre o stream sincronizado inteiro
    (todas as atividades concatenadas, na ordem gravada) - mesmo padrao do
    projeto WESAD anterior. Uma janela vira linha so se pelo menos
    `purity_threshold` das amostras dentro dela forem da mesma atividade
    (descarta janelas de transicao entre atividades).
    """
    synced = _sync_accel_gyro(accel_df, gyro_df)

    window_size = int(window_seconds * fs)
    n_total = len(synced)
    n_windows = n_total // window_size

    columns = [name for name, _ in FEATURE_COLUMNS]
    rows: list[dict[str, Any]] = []

    for w in range(n_windows):
        start = w * window_size
        end = start + window_size
        window_df = synced.iloc[start:end]

        codes, counts = np.unique(window_df["activity_code"].to_numpy(), return_counts=True)
        majority_code = str(codes[np.argmax(counts)])
        purity = float(counts.max() / window_size)

        if majority_code not in ACTIVITY_LABELS or purity < purity_threshold:
            continue

        row: dict[str, Any] = {
            "window_id": w,
            "window_start_sample": start,
            "n_samples": window_size,
            "activity_label": majority_code,
            "label_purity": purity,
        }

        for name in ("accel_x", "accel_y", "accel_z", "gyro_x", "gyro_y", "gyro_z"):
            mean, std, mn, mx = _window_stats(window_df[name].to_numpy())
            row[f"{name}_mean"] = mean
            row[f"{name}_std"] = std
            row[f"{name}_min"] = mn
            row[f"{name}_max"] = mx

        rows.append(row)

    return pd.DataFrame(rows, columns=columns)

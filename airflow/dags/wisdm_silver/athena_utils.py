"""Helpers finos sobre boto3 para rodar DDL no Athena e esperar o resultado.

Usados pela DAG para registrar a tabela externa e as particoes da camada
Silver no Glue Data Catalog via `ALTER TABLE ... ADD PARTITION`, evitando
`MSCK REPAIR TABLE` (mais lento, pois varre o prefixo inteiro do S3 a cada
chamada).
"""

from __future__ import annotations

import time


class AthenaQueryError(RuntimeError):
    pass


def run_athena_query(
    athena_client,
    sql: str,
    database: str,
    output_location: str,
    workgroup: str = "primary",
    poll_seconds: float = 2.0,
    timeout_seconds: float = 120.0,
) -> str:
    response = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=workgroup,
    )
    query_execution_id = response["QueryExecutionId"]

    waited = 0.0
    while waited < timeout_seconds:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return query_execution_id
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "sem detalhes"
            )
            raise AthenaQueryError(
                f"Query Athena {state.lower()}: {reason}\nSQL: {sql}"
            )
        time.sleep(poll_seconds)
        waited += poll_seconds

    raise AthenaQueryError(
        f"Timeout esperando query Athena terminar (id={query_execution_id})"
    )

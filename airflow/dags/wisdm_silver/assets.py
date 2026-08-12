"""Asset(s) do Airflow compartilhados entre as 3 DAGs da medalhao (bronze,
silver, gold).

Um Asset (Airflow 3, antigo "Dataset") e a forma nativa de expressar uma
dependencia de DADOS entre DAGs, em vez de uma dependencia de TEMPO (cron) ou
um trigger explicito (`TriggerDagRunOperator`). Cada DAG produtora declara um
Asset como `outlet` da sua ultima task bem-sucedida; a DAG consumidora usa
`schedule=[ASSET]`, ou seja, e disparada automaticamente pelo scheduler assim
que a produtora terminar com sucesso - sem polling, sem acoplar uma DAG a
outra por `dag_id`.

Encadeamento completo, na ordem pedida pelo usuario:
    kaggle_wisdm_to_s3 (upload_to_s3 outlet=BRONZE_S3_ASSET)
    -> wisdm_bronze_to_silver (schedule=[BRONZE_S3_ASSET]; sua task run_dbt
       outlet=SILVER_DBT_ASSET)
    -> wisdm_silver_to_gold (schedule=[SILVER_DBT_ASSET])

Disparar so a `kaggle_wisdm_to_s3` (manual ou pelo cron diario) cascateia
automaticamente pelas 3 camadas, cada uma so depois que a anterior realmente
terminou com sucesso - as tasks de verificacao (`verify_bronze_ready`,
`verify_silver_ready`) continuam valendo em cada etapa, entao um bronze ou
silver incompletos ainda barram a cadeia em vez de deixar a proxima camada
seguir em cima de dado parcial.

Fica num modulo proprio (em vez de definido dentro de cada arquivo de DAG)
para garantir que produtora e consumidora de cada Asset referenciam
exatamente o mesmo objeto (Airflow casa produtor/consumidor pela URI do
Asset, entao os dois lados precisam apontar pro mesmo valor).
"""

from airflow.sdk import Asset

# uri (default = name quando uri nao e passado) e usada pelo scheduler para
# casar quem produz com quem consome - mantenha o mesmo valor nos dois lados.
BRONZE_S3_ASSET = Asset("wisdm_bronze_s3_raw")
SILVER_DBT_ASSET = Asset("wisdm_silver_dbt_models")

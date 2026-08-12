# Registro de custos — AWS

## Metodologia

Este projeto roda em conta **AWS Academy Learner Lab**, que não expõe Cost
Explorer nem Billing ao aluno (créditos de laboratório, não cartão de
crédito real) — não há fatura para citar. Por isso, o registro abaixo é uma
**estimativa via AWS Pricing Calculator** (taxas públicas de `us-east-1`,
consultadas em aws.amazon.com/*/pricing/), aplicada sobre o **uso real
medido neste repositório** (volume de dados, nº de arquivos, nº de
modelos/testes dbt) — não são números inventados.

Há duas partes:

1. **Custo real da solução implementada** (S3 + Glue Data Catalog + Athena
   — o que de fato roda contra a AWS hoje).
2. **Custo estimado da arquitetura proposta 100% AWS** (seção 4.5 do
   diagrama — [`diagrama_arquitetura_aws.md`](./diagrama_arquitetura_aws.md)),
   caso a solução fosse produtizada rodando continuamente na nuvem, sem
   depender do Docker Compose local.

## Custo real (solução implementada hoje)

Uso medido diretamente no repositório e no dataset baixado:

- **Bronze** (`raw/wisdm/`): dump completo do `kagglehub.dataset_download`
  (todo o dataset WISDM, não só os 4 arquivos/sujeito usados pela Silver —
  ver `airflow/dags/kaggle_wisdm_to_s3.py`) — **464 arquivos, 896 MiB**
  (medido com `du -sh` no dataset baixado localmente).
- **Silver** (`silver/wisdm_features/subject=<id>/`): 1 Parquet por sujeito,
  51 arquivos, ~5 MB no total (cache local equivalente,
  `ml/data/fct_wisdm_windows.parquet`, tem 3,9 MB para as 16.721 linhas
  combinadas).
- **Gold**: 907 linhas (`gold_subject_activity_summary`) + 18 linhas
  (`gold_activity_benchmark`) — desprezível (<1 MB).
- **Execuções completas documentadas**: 2 (README, seção "Checklist de
  entrega") — usado como base para custo de requisições, além de uma
  margem de desenvolvimento/testes manuais.
- **dbt**: 5 models (`stg_wisdm_features`, `dim_subject`,
  `fct_wisdm_windows`, `gold_subject_activity_summary`,
  `gold_activity_benchmark`) + 23 testes (`unique`/`not_null`/
  `accepted_values`) — cada `dbt run`/`dbt test` dispara 1 query Athena por
  model/teste.

| Serviço | Recurso | Uso medido/estimado | Preço unitário (`us-east-1`) | Custo/mês |
|---|---|---|---|---|
| S3 Standard | Armazenamento (bronze+silver+gold) | ~0,88 GB | $0,023/GB-mês | $0,02 |
| S3 | Requisições PUT/COPY/POST/LIST | (464 bronze + 51 silver) × 2 execuções = 1.030 | $0,005/1.000 req | $0,01 |
| S3 | Requisições GET | (2 arquivos/sujeito × 51) × 2 execuções = 204 | $0,0004/1.000 req | <$0,01 |
| Athena | Dados escaneados | ~200 queries (dbt run+test documentados + desenvolvimento), mínimo de 10 MB/query → ~2 GB | $5/TB escaneado | $0,01 |
| Glue Data Catalog | Objetos armazenados + acessos | 2 bancos, <10 tabelas, <1.000 acessos | 1º milhão de objetos e acessos grátis | $0,00 |
| CloudFormation | Provisionamento (S3 + Glue) | — | Sem custo pelo serviço em si | $0,00 |

**Total real estimado: ~$0,04/mês.**

Dentro do free tier da maioria das contas AWS novas; na prática, consumido
como créditos do AWS Academy Learner Lab — não é cobrado do grupo. O maior
componente de custo é o armazenamento do dump bruto de 896 MiB na camada
Bronze, ainda assim irrisório.

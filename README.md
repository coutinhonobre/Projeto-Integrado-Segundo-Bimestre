# Projeto Integrado — Reconhecimento de Atividade Humana (WISDM)

Projeto final do módulo 2 da Pós-graduação em Inteligência Artificial (IFG). Pipeline de dados completo, da ingestão bruta ao dashboard, sobre o **WISDM Smartphone and Smartwatch Activity and Biometrics Dataset**, seguido de um modelo de machine learning para reconhecer, a partir de dados de acelerômetro e giroscópio de smartwatch, qual das 18 atividades (andar, correr, comer, escovar os dentes etc.) uma pessoa está realizando.

- 🎥 Apresentação: https://www.youtube.com/watch?v=D4Ny5alpRMk
- 📊 Slides: [ApresentacaoIgorCoutinhoFerreiraNobre.pdf](ApresentacaoIgorCoutinhoFerreiraNobre.pdf)
- 📄 Relatório: [RelatorioIgorCoutinhoFerreiraNobre.pdf](RelatorioIgorCoutinhoFerreiraNobre.pdf)

## Dataset

WISDM Smartphone and Smartwatch Activity and Biometrics Dataset, dados de acelerômetro e giroscópio de smartphone e smartwatch, coletados de 51 sujeitos realizando 18 atividades.

- UCI: https://archive.ics.uci.edu/dataset/507/wisdm+smartphone+and+smartwatch+activity+and+biometrics+dataset
- Kaggle: https://www.kaggle.com/datasets/mashlyn/smartphone-and-smartwatch-activity-and-biometrics

## Arquitetura

Pipeline em arquitetura medalhão (Bronze / Silver / Gold) orquestrado pelo Airflow, com transformações em dbt sobre Athena/Glue e visualização no Metabase:

```
Kaggle → S3 (Bronze, raw)  --[Airflow + dbt]-->  Athena/Glue (Silver, features por janela)
       --[dbt]--> Athena/Glue (Gold, agregados)  --[Airflow]--> Metabase (dashboard)

Athena (Silver) --[notebook]--> MLP (numpy) --[export]--> Gold (resultados do modelo) → Metabase
```

Diagrama completo em [arquitetura/diagrama_arquitetura_simplificado.drawio.png](arquitetura/diagrama_arquitetura_simplificado.drawio.png).

## Estrutura do repositório

| Pasta | Conteúdo |
|---|---|
| [`airflow/`](airflow) | DAGs da orquestração: ingestão do Kaggle para o S3, Bronze→Silver→Gold via dbt, e provisionamento do dashboard no Metabase |
| [`dbt/`](dbt) | Modelos dbt (staging, marts e gold) que rodam sobre Athena/Glue |
| [`arquitetura/`](arquitetura) | Diagrama da arquitetura do pipeline |
| [`infra/`](infra) | Provisionamento da infraestrutura AWS (bucket S3 + bancos Glue) via CloudFormation |
| [`metabase/`](metabase) | Scripts de setup automático e montagem do dashboard no Metabase |
| [`ml/`](ml) | Notebook com o modelo MLP (implementado do zero em numpy, comparado ao `MLPClassifier` do sklearn) e publicação dos resultados na camada Gold |
| [`latex/`](latex) | Fonte LaTeX do relatório |

## Como rodar

### 1. Pré-requisitos

- Docker e Docker Compose
- Conta AWS com acesso a S3, Glue e Athena (compatível com sandbox do AWS Academy)
- Conta Kaggle (para a DAG de ingestão)

### 2. Configuração

Copie `.env.example` para `.env` e preencha as variáveis (credenciais Kaggle, credenciais AWS, nome do bucket S3, chaves do Airflow etc. — cada variável está documentada no próprio arquivo).

### 3. Infraestrutura AWS

```bash
./infra/deploy_infra.sh
```

Provisiona o bucket S3 e os bancos Glue (Silver e Gold). Copie os outputs do stack para as variáveis correspondentes no `.env`.

### 4. Subir Airflow + Metabase

```bash
docker compose up --build
```

- Airflow: http://localhost:8080
- Metabase: http://localhost:3000

Execute as DAGs na ordem: `kaggle_wisdm_to_s3` → `wisdm_bronze_to_silver` → `wisdm_silver_to_gold` → `wisdm_metabase_dashboard`.

### 5. Notebook de Machine Learning

```bash
cd ml
docker compose up --build
```

Abra a URL do Jupyter Lab exibida no log e rode `mlp_activity_classification.ipynb`. A última célula publica os resultados do modelo na camada Gold e atualiza o dashboard do Metabase.

Um parquet de features (cache do Athena) já está versionado em `ml/data/`, então o notebook roda mesmo sem sessão AWS ativa.

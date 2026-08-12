FROM apache/airflow:3.3.0

# dbt-athena-community fixa mmh3<4.2.0, versao sem wheel pre-compilado para
# Python 3.13/aarch64 - precisa compilar a extensao C na instalacao, por isso
# build-essential (gcc) precisa estar presente na imagem.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Imagem so para o Jupyter Lab do modelo de ML — separada da imagem do
# Airflow (Dockerfile na raiz) porque este codigo roda fora do container do
# Airflow, com um Python mais novo e um conjunto de deps proprio (numpy,
# scikit-learn, jupyterlab) que o Airflow nao precisa.
FROM python:3.13-slim

WORKDIR /workspace/ml

COPY ml/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.token=''", "--ServerApp.password=''"]

#!/usr/bin/env bash
# Provisiona a infraestrutura AWS (bucket S3 + bancos Glue) via CloudFormation,
# antes de subir os containers — evita que ela seja criada como efeito
# colateral da primeira execucao da DAG. Ver infra/cloudformation.yaml.
#
# Uso (a partir da raiz do repo): ./infra/deploy_infra.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

set -a
source .env
set +a

: "${S3_BUCKET_NAME:?defina S3_BUCKET_NAME no .env}"
STACK_NAME="projeto-integrado-wisdm"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "Validando template..."
aws cloudformation validate-template \
  --template-body file://infra/cloudformation.yaml \
  --region "$REGION" >/dev/null

echo "Deployando stack '$STACK_NAME' (bucket=$S3_BUCKET_NAME, regiao=$REGION)..."
aws cloudformation deploy \
  --template-file infra/cloudformation.yaml \
  --stack-name "$STACK_NAME" \
  --parameter-overrides BucketName="$S3_BUCKET_NAME" \
  --region "$REGION"

echo
echo "Outputs (copie para o .env: SilverGlueDatabaseName -> GLUE_DATABASE_NAME, GoldGlueDatabaseName -> GOLD_GLUE_DATABASE_NAME):"
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs" --output table

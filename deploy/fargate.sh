#!/usr/bin/env bash
# Build + push Trade Flow to ECR. After an admin attaches ECS/IAM/ELB
# permissions, the same script can finish the Fargate service.
set -euo pipefail
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY AWS_PROXY AWS_HTTPS_PROXY || true

REGION="${AWS_REGION:-ap-south-1}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
REPO="tradeflow-fetch"
IMAGE="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/${REPO}:latest"
CLUSTER="tradeflow"
SERVICE="tradeflow-fetch"
LOG_GROUP="/ecs/tradeflow-fetch"
TASK_FAMILY="tradeflow-fetch"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> AWS account ${ACCOUNT} region ${REGION}"

aws ecr describe-repositories --repository-names "${REPO}" --region "${REGION}" >/dev/null 2>&1 \
  || aws ecr create-repository --repository-name "${REPO}" --region "${REGION}" >/dev/null
echo "==> ECR ${IMAGE}"

aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"

docker build --platform linux/amd64 -t "${REPO}:latest" -t "${IMAGE}" "${ROOT}"
docker push "${IMAGE}"
echo "==> Image pushed. Fargate next needs ECS + IAM task role + ELB."

if ! aws ecs list-clusters --region "${REGION}" >/dev/null 2>&1; then
  echo ""
  echo "STOP: IAM user cannot call ecs:* yet."
  echo "Ask an account admin to attach:"
  echo "  AmazonECS_FullAccess"
  echo "  ElasticLoadBalancingFullAccess"
  echo "  CloudWatchLogsFullAccess"
  echo "  IAM: create role ecsTaskExecutionRole + AmazonECSTaskExecutionRolePolicy"
  echo "Then re-run: bash deploy/fargate.sh --service"
  exit 0
fi

if [[ "${1:-}" != "--service" ]]; then
  echo "Image is ready. Re-run with --service after IAM is granted."
  exit 0
fi

echo "Create the cluster/service from the AWS console steps, or extend this script once ecs:* is allowed."

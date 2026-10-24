#!/usr/bin/env bash
# Build the Trade Flow image for linux/amd64 and push it to the fleet's ECR repo
# (created by TradeFlowFleet-<env> in the dashboard repo's infra/). Then roll the
# services so they pull it.
#
#   bash deploy/push-image.sh dev            # tag = git short sha, also :latest
#   bash deploy/push-image.sh prod v1.2.0
#
# Needs: docker, aws cli with the tradeflow-admin credentials, region ap-south-1.
set -euo pipefail
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY || true

ENV_NAME="${1:-dev}"
TAG="${2:-$(git rev-parse --short HEAD)}"
REGION="${AWS_REGION:-ap-south-1}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
REPO="tradeflow-${ENV_NAME}"
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLUSTER="tradeflow-${ENV_NAME}"

echo "==> ${REGISTRY}/${REPO}:${TAG} (and :latest)"
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${REGISTRY}"
docker build --platform linux/amd64 -t "${REGISTRY}/${REPO}:${TAG}" -t "${REGISTRY}/${REPO}:latest" "${ROOT}"
docker push "${REGISTRY}/${REPO}:${TAG}"
docker push "${REGISTRY}/${REPO}:latest"

echo "==> rolling services on ${CLUSTER} to the new :latest"
for svc in $(aws ecs list-services --cluster "${CLUSTER}" --region "${REGION}" --query 'serviceArns[]' --output text 2>/dev/null); do
  name="${svc##*/}"
  aws ecs update-service --cluster "${CLUSTER}" --service "${name}" --force-new-deployment --region "${REGION}" --query 'service.serviceName' --output text
done
echo "==> done. Workers at desired 0 pick the new image when the backlog next scales them out."

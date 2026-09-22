#!/usr/bin/env bash
# Run this once as an account admin (not ordergenie-dev).
# It creates the Fargate execution role and grants this IAM user enough to finish deploy.
set -euo pipefail
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY || true

REGION="${AWS_REGION:-ap-south-1}"
USER_NAME="${1:-ordergenie-dev}"
ROLE_NAME="ecsTaskExecutionRole"

aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1 || aws iam create-role \
  --role-name "${ROLE_NAME}" \
  --assume-role-policy-document '{
    "Version":"2012-10-17",
    "Statement":[{
      "Effect":"Allow",
      "Principal":{"Service":"ecs-tasks.amazonaws.com"},
      "Action":"sts:AssumeRole"
    }]
  }'

aws iam attach-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy

for policy in \
  AmazonECS_FullAccess \
  ElasticLoadBalancingFullAccess \
  CloudWatchLogsFullAccess
do
  aws iam attach-user-policy \
    --user-name "${USER_NAME}" \
    --policy-arn "arn:aws:iam::aws:policy/${policy}"
done

echo "Done. ${USER_NAME} can now create the ECS cluster, ALB, and Fargate service in ${REGION}."
echo "Then run: bash deploy/fargate.sh --service"

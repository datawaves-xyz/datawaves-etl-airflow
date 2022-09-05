#!/bin/bash
EKS_HOST=149671120510.dkr.ecr.ap-northeast-1.amazonaws.com
tag=2.3.2-3.1.2
image_name="$EKS_HOST"/airflow-spark:"$tag"

echo "$image_name"

aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin "$EKS_HOST"
docker buildx build --push -t "$image_name" -f deployments/base/Dockerfile .
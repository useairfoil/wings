#!/usr/bin/env bash
set -euo pipefail

GRPC_ADDRESS="${WINGS_GRPC_ADDRESS:-127.0.0.1:7253}"
SEAWEEDFS_ENDPOINT="${SEAWEEDFS_ENDPOINT:-http://localhost:8333}"
SEAWEEDFS_BUCKET="${SEAWEEDFS_BUCKET:-default-bucket}"
SEAWEEDFS_REGION="${SEAWEEDFS_REGION:-us-east-1}"
SEAWEEDFS_ACCESS_KEY_ID="${SEAWEEDFS_ACCESS_KEY_ID:-wingsdevaccesskey}"
SEAWEEDFS_SECRET_ACCESS_KEY="${SEAWEEDFS_SECRET_ACCESS_KEY:-wingsdevsecretkey}"

grpcurl \
  -plaintext \
  -d @ \
  "${GRPC_ADDRESS}" \
  wings.cluster.ClusterService/CreateNamespace <<EOF
{
  "namespaceId": "default",
  "namespace": {
    "objectStore": {
      "s3Compatible": {
        "bucketName": "${SEAWEEDFS_BUCKET}",
        "accessKeyId": "${SEAWEEDFS_ACCESS_KEY_ID}",
        "secretAccessKey": "${SEAWEEDFS_SECRET_ACCESS_KEY}",
        "endpoint": "${SEAWEEDFS_ENDPOINT}",
        "region": "${SEAWEEDFS_REGION}",
        "allowHttp": true
      }
    },
    "lake": {
      "parquet": {}
    }
  }
}
EOF

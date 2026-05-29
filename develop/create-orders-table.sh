#!/usr/bin/env bash
set -euo pipefail

GRPC_ADDRESS="${WINGS_GRPC_ADDRESS:-127.0.0.1:7253}"
NAMESPACE="${WINGS_NAMESPACE:-default}"

grpcurl \
  -plaintext \
  -d @ \
  "${GRPC_ADDRESS}" \
  wings.cluster.ClusterService/CreateTable <<EOF
{
  "parent": "namespaces/${NAMESPACE}",
  "tableId": "orders",
  "table": {
    "schema": {
      "fields": [
        {
          "name": "orderkey",
          "id": "0",
          "arrowType": { "int64": {} },
          "nullable": false
        },
        {
          "name": "custkey",
          "id": "1",
          "arrowType": { "int64": {} },
          "nullable": false
        },
        {
          "name": "orderstatus",
          "id": "2",
          "arrowType": { "utf8": {} },
          "nullable": false
        },
        {
          "name": "totalprice",
          "id": "3",
          "arrowType": { "float64": {} },
          "nullable": false
        },
        {
          "name": "orderdate",
          "id": "4",
          "arrowType": { "date32": {} },
          "nullable": false
        },
        {
          "name": "orderpriority",
          "id": "5",
          "arrowType": { "utf8": {} },
          "nullable": false
        },
        {
          "name": "clerk",
          "id": "6",
          "arrowType": { "utf8": {} },
          "nullable": false
        },
        {
          "name": "shippriority",
          "id": "7",
          "arrowType": { "int32": {} },
          "nullable": false
        },
        {
          "name": "comment",
          "id": "8",
          "arrowType": { "utf8": {} },
          "nullable": false
        }
      ]
    },
    "description": "TPC-H orders table",
    "keyFieldId": "0",
    "versionFieldId": "4"
  }
}
EOF

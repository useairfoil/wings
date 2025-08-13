#!/usr/bin/env bash

set -exu

WINGS_BIN=${WINGS_BIN:-./target/debug/wings}

echo "Creating score topic"
$WINGS_BIN admin create-topic \
    tenants/default/namespaces/default/topics/score \
    'id:u64' 'first_name:string' 'score:u64' \
    --partition id

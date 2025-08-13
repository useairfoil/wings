#!/usr/bin/env bash

set -exu

WINGS_BIN=${WINGS_BIN:-./target/debug/wings}

input=$1
partition=$2
echo "Pushing data to score(${partition}) from file ${input}"
$WINGS_BIN push tenants/default/namespaces/default score $partition "@${input}"

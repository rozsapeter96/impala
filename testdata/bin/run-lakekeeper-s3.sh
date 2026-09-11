#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Starts the Lakekeeper S3 (RustFS) test stack for Iceberg credential-vending tests.
# Services bind to localhost so Impala (outside Docker) can reach RustFS :9100 and
# Lakekeeper :8181.

set -euo pipefail

# The REST catalog machinery requires Iceberg >= 1.5.
IFS='.-' read -r major minor _ <<< "$IMPALA_ICEBERG_VERSION"
if (( major < 1 )) || { (( major == 1 )) && (( minor < 5 )); }; then
    echo "Iceberg version does NOT meet requirement (need at least 1.5):" \
         "$IMPALA_ICEBERG_VERSION"
    exit 1
fi

cd "${IMPALA_HOME}/testdata/bin/minicluster_lakekeeper_s3"

echo "Starting Lakekeeper S3 (RustFS) test stack..."
# --build picks up bootstrap image changes (Dockerfile / seed script).
docker compose up -d --build --wait

# The one-shot 'bootstrap' service (setup.sh) creates the warehouse/namespace/table but
# has no healthcheck, so '--wait' does not block on it. Wait for it explicitly, else
# Impala may connect before the warehouse exists.
echo "Waiting for warehouse bootstrap to complete..."
BOOTSTRAP_CONTAINER=$(docker compose ps -aq bootstrap)
if [[ -z "${BOOTSTRAP_CONTAINER}" ]]; then
    echo "ERROR: bootstrap container not found."
    exit 1
fi
BOOTSTRAP_RC=$(docker wait "${BOOTSTRAP_CONTAINER}")
if [[ "${BOOTSTRAP_RC}" != "0" ]]; then
    echo "ERROR: warehouse bootstrap failed (exit code ${BOOTSTRAP_RC}). Logs:"
    docker compose logs bootstrap
    exit 1
fi

echo "Lakekeeper S3 stack is ready."
echo "  RustFS S3 API:   http://localhost:9100"
echo "  Lakekeeper REST: http://localhost:8181"
echo "  Keycloak:        http://localhost:7071"

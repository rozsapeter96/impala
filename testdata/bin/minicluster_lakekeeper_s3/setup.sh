#!/bin/sh
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
# Bootstraps the Lakekeeper S3 test environment:
#   1. Creates the test bucket in RustFS.
#   2. Obtains an admin token from Keycloak.
#   3. Bootstraps Lakekeeper (accept terms).
#   4. Creates the S3-backed test warehouse.
#   5. Creates a test namespace and Iceberg table for Impala to scan.

LAKEKEEPER_URL="http://localhost:8181"
KEYCLOAK_URL="http://localhost:7071"
WAREHOUSE_NAME="impala_s3_test"
S3_URL="http://localhost:9100"
S3_BUCKET="impala-iceberg-test"
S3_ROOT_USER="rustfs-root-user"
S3_ROOT_PASSWORD="rustfs-root-password"

echo "Waiting for services to be ready..."
sleep 5

# Plain S3 PUT Bucket, signed with curl's built-in SigV4 support, so no vendor CLI is
# needed. 409 means the bucket already exists from an earlier run, which is fine.
echo "Creating S3 bucket '${S3_BUCKET}'..."
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "${S3_ROOT_USER}:${S3_ROOT_PASSWORD}" \
  "${S3_URL}/${S3_BUCKET}")
if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "409" ]; then
  echo "ERROR: Bucket creation failed (HTTP ${HTTP_CODE})"
  exit 1
fi
echo "Bucket ready."

echo "Getting admin token from Keycloak..."
TOKEN=$(curl -s -X POST \
  "${KEYCLOAK_URL}/realms/lakekeeper-realm/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=lakekeeper-admin" \
  -d "password=password" \
  -d "grant_type=password" \
  -d "client_id=lakekeeper-client" | jq -r '.access_token')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "ERROR: Failed to get token from Keycloak"
  exit 1
fi
echo "Token acquired."

echo "Bootstrapping Lakekeeper..."
curl -f -s -X POST "${LAKEKEEPER_URL}/management/v1/bootstrap" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"accept-terms-of-use": true}' \
  -o /dev/null
if [ $? -ne 0 ]; then echo "Bootstrap failed!"; exit 1; fi
echo "Bootstrap successful."

echo "Creating S3 warehouse..."
curl -f -s -X POST "${LAKEKEEPER_URL}/management/v1/warehouse" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data "@/create-s3-warehouse.json" \
  -o /dev/null
if [ $? -ne 0 ]; then echo "Warehouse creation failed!"; exit 1; fi
echo "Warehouse created."

# The Iceberg REST catalog uses the warehouse-id (not the name) as the {prefix} segment.
echo "Resolving catalog prefix for warehouse '${WAREHOUSE_NAME}'..."
PREFIX=$(curl -f -s \
  "${LAKEKEEPER_URL}/catalog/v1/config?warehouse=${WAREHOUSE_NAME}" \
  -H "Authorization: Bearer $TOKEN" | jq -r '.defaults.prefix')
if [ -z "$PREFIX" ] || [ "$PREFIX" = "null" ]; then
  echo "Failed to resolve catalog prefix!"
  exit 1
fi
CATALOG_URL="${LAKEKEEPER_URL}/catalog/v1/${PREFIX}"
echo "Catalog prefix: ${PREFIX}"

echo "Creating test namespace 'ice_s3'..."
curl -f -s -X POST "${CATALOG_URL}/namespaces" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"namespace": ["ice_s3"]}' \
  -o /dev/null
if [ $? -ne 0 ]; then echo "Namespace creation failed!"; exit 1; fi
echo "Namespace created."

# Create the table with an explicit s3a:// location. Lakekeeper auto-assigns s3://, but
# allow-alternative-protocols lets it accept s3a://, which Impala's S3AFileSystem and
# Iceberg's HadoopFileIO resolve natively (no scheme mapping needed).
TABLE_LOCATION="s3a://impala-iceberg-test/warehouse/ice_s3/nation"
echo "Creating test table 'ice_s3.nation' at ${TABLE_LOCATION}..."
curl -f -s -X POST "${CATALOG_URL}/namespaces/ice_s3/tables" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data "{
    \"name\": \"nation\",
    \"location\": \"${TABLE_LOCATION}\",
    \"schema\": {
      \"type\": \"struct\",
      \"schema-id\": 0,
      \"fields\": [
        {\"id\": 1, \"name\": \"n_nationkey\", \"required\": false, \"type\": \"int\"},
        {\"id\": 2, \"name\": \"n_name\",      \"required\": false, \"type\": \"string\"},
        {\"id\": 3, \"name\": \"n_regionkey\", \"required\": false, \"type\": \"int\"},
        {\"id\": 4, \"name\": \"n_comment\",   \"required\": false, \"type\": \"string\"}
      ]
    },
    \"partition-spec\": {\"spec-id\": 0, \"fields\": []},
    \"write-order\": {\"order-id\": 0, \"fields\": []},
    \"properties\": {}
  }" \
  -o /dev/null
if [ $? -ne 0 ]; then echo "Table creation failed!"; exit 1; fi
echo "Table created."

# A second table seeded with many small files (one per append). A scan of it must resolve
# vended credentials for every file, exercising the per-file credential path at scale.
MANY_FILES_LOCATION="s3a://impala-iceberg-test/warehouse/ice_s3/many_files"
echo "Creating test table 'ice_s3.many_files' at ${MANY_FILES_LOCATION}..."
curl -f -s -X POST "${CATALOG_URL}/namespaces/ice_s3/tables" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data "{
    \"name\": \"many_files\",
    \"location\": \"${MANY_FILES_LOCATION}\",
    \"schema\": {
      \"type\": \"struct\",
      \"schema-id\": 0,
      \"fields\": [
        {\"id\": 1, \"name\": \"id\",  \"required\": false, \"type\": \"int\"},
        {\"id\": 2, \"name\": \"val\", \"required\": false, \"type\": \"string\"}
      ]
    },
    \"partition-spec\": {\"spec-id\": 0, \"fields\": []},
    \"write-order\": {\"order-id\": 0, \"fields\": []},
    \"properties\": {}
  }" \
  -o /dev/null
if [ $? -ne 0 ]; then echo "many_files table creation failed!"; exit 1; fi
echo "many_files table created."

echo "Seeding test tables 'ice_s3.nation' and 'ice_s3.many_files' with data..."
python3 /seed-table.py
if [ $? -ne 0 ]; then echo "Table seeding failed!"; exit 1; fi
echo "Table seeded."

echo "Setup complete!"

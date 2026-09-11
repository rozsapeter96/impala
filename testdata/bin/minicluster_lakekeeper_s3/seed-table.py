#!/usr/bin/env python3
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
# Seeds ice_s3.nation with a small data file via the Lakekeeper REST catalog so scans
# read a real S3 object using vended credentials. Runs in the bootstrap container.

import sys

import pyarrow as pa
import requests
from pyiceberg.catalog.rest import RestCatalog

LAKEKEEPER_URI = "http://localhost:8181/catalog"
TOKEN_URI = ("http://localhost:7071/realms/lakekeeper-realm/"
             "protocol/openid-connect/token")
WAREHOUSE = "impala_s3_test"


def fetch_token():
    # Fetch the token from Keycloak ourselves; pyiceberg 0.7.1 would POST to the
    # catalog's built-in /v1/oauth/tokens, which Lakekeeper does not serve.
    resp = requests.post(
        TOKEN_URI,
        data={
            "grant_type": "client_credentials",
            "client_id": "impala-client",
            "client_secret": "impala-client-secret",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# Number of data files to seed into ice_s3.many_files. Each append() writes a new data
# file, so this many separate appends produce this many files -- exercising per-file
# credential resolution/refresh in the backend for a single scan.
MANY_FILES_COUNT = 100


def seed_nation(catalog):
    table = catalog.load_table(("ice_s3", "nation"))

    data = pa.table({
        "n_nationkey": pa.array([1, 2], type=pa.int32()),
        "n_name": pa.array(["ALGERIA", "ARGENTINA"], type=pa.string()),
        "n_regionkey": pa.array([0, 1], type=pa.int32()),
        "n_comment": pa.array(["a", "b"], type=pa.string()),
    })

    table.append(data)
    print("Seeded ice_s3.nation with %d rows." % data.num_rows)


def seed_many_files(catalog):
    # Append one row at a time so each commit produces a distinct data file. The scan of
    # this table must resolve (and, under a low refresh threshold, refresh) credentials
    # for every one of these files.
    table = catalog.load_table(("ice_s3", "many_files"))
    for i in range(MANY_FILES_COUNT):
        data = pa.table({
            "id": pa.array([i], type=pa.int32()),
            "val": pa.array(["v%d" % i], type=pa.string()),
        })
        table.append(data)
    print("Seeded ice_s3.many_files with %d files." % MANY_FILES_COUNT)


def main():
    token = fetch_token()
    catalog = RestCatalog(
        "lakekeeper",
        **{
            "uri": LAKEKEEPER_URI,
            "warehouse": WAREHOUSE,
            "token": token,
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
        },
    )

    seed_nation(catalog)
    seed_many_files(catalog)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print("ERROR seeding table: %s" % e, file=sys.stderr)
        sys.exit(1)

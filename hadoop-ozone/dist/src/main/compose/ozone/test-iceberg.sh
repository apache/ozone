#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#suite:integration

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=false
export OZONE_REPLICATION_FACTOR=3
export COMPOSE_FILE=docker-compose.yaml:iceberg.yaml:trino.yaml

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env 3

export BUCKET=warehouse

execute_command_in_container s3g ozone sh bucket create --layout OBJECT_STORE /s3v/${BUCKET}

execute_command_in_container spark-iceberg spark-shell <<EOF
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
val schema = StructType( Array(
    StructField("vendor_id", LongType,true),
    StructField("trip_id", LongType,true),
    StructField("trip_distance", FloatType,true),
    StructField("fare_amount", DoubleType,true),
    StructField("store_and_fwd_flag", StringType,true)
))
spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
    .writeTo("demo.nyc.taxis").create()

val data = Seq(
    Row(1: Long, 1000371: Long, 1.8f: Float, 15.32: Double, "N": String),
    Row(2: Long, 1000372: Long, 2.5f: Float, 22.15: Double, "N": String),
    Row(2: Long, 1000373: Long, 0.9f: Float, 9.01: Double, "N": String),
    Row(1: Long, 1000374: Long, 8.4f: Float, 42.13: Double, "Y": String)
)
spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    .writeTo("demo.nyc.taxis").append()

spark.table("demo.nyc.taxis").show()
EOF

execute_command_in_container trino trino <<EOF
DESCRIBE iceberg.nyc.taxis;
INSERT INTO iceberg.nyc.taxis VALUES (2, 1000375, 7.2, 555, 'N');
SELECT * FROM iceberg.nyc.taxis;
EOF

execute_robot_test scm -v BUCKET:${BUCKET} integration/iceberg.robot

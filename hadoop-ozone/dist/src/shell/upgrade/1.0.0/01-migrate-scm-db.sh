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

echo Running upgrade script for HDDS-3499

ldb --db=scm.db create_column_family containers
ldb --db=scm.db create_column_family pipelines

ldb --db=scm-container.db --key_hex --value_hex dump | ldb --db=scm.db --key_hex --value_hex --column_family=containers load
ldb --db=scm-pipeline.db --key_hex --value_hex dump | ldb --db=scm.db --key_hex --value_hex --column_family=pipelines load

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/**
 * RocksDB is deprecating the RocksObject's finalizer that cleans up native
 * resources. In fact, the finalizer is removed in the new version of RocksDB
 * as per https://github.com/facebook/rocksdb/commit/99d86252b. That poses a
 * requirement for RocksDb's applications to explicitly close RocksObject
 * instances themselves to avoid leaking native resources. The general approach
 * is to close RocksObjects with try-with-resource statement.
 * Yet, this is not always an easy option in Ozone we need a mechanism to
 * manage and detect leaks.
 *
 * This package contains wrappers and utilities to catch RocksObject
 * instantiates in Ozone, intercept their finalizers and assert if the created
 * instances are closed properly before being GCed.
 */
package org.apache.hadoop.hdds.utils.db.managed;


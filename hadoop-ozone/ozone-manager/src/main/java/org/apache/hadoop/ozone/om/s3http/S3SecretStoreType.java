/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.s3http;

import java.util.Objects;

/**
 * S3 secret storage type enum.
 */
public enum S3SecretStoreType {
  ROCKSDB("rocksDB"),
  HASHICORP_VAULT("vault");

  private final String name;

  S3SecretStoreType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static S3SecretStoreType fromName(String name) {
    for (S3SecretStoreType value : values()) {
      if (Objects.equals(value.name, name)) {
        return value;
      }
    }
    return null;
  }
}

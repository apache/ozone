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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.s3.remote.S3SecretStoreType;
import org.apache.hadoop.ozone.s3.remote.vault.S3RemoteSecretStore;

import java.io.IOException;

import static org.apache.hadoop.ozone.s3.remote.S3SecretStoreConfigurationKeys.DEFAULT_SECRET_STORAGE_TYPE;
import static org.apache.hadoop.ozone.s3.remote.S3SecretStoreConfigurationKeys.S3_SECRET_STORAGE_TYPE;

/**
 * Implementation of S3 secret store provider.
 */
public class SecretStoreProviderImpl implements SecretStoreProvider {
  private final OmMetadataManagerImpl manager;

  /**
   * Constructor.
   *
   * @param manager Ozone metadata manager.
   */
  public SecretStoreProviderImpl(OmMetadataManagerImpl manager) {
    this.manager = manager;
  }

  public S3SecretStore get(Configuration conf) throws IOException {
    S3SecretStoreType type = S3SecretStoreType.fromType(
        conf.get(S3_SECRET_STORAGE_TYPE, DEFAULT_SECRET_STORAGE_TYPE));
    switch (type) {
    case HASHICORP_VAULT:
      return S3RemoteSecretStore.fromConf(conf);
    case ROCKSDB:
    default:
      return manager;
    }
  }
}

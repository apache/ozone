/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.security;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.ozone.om.S3Batcher;
import org.apache.hadoop.ozone.om.S3SecretStore;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;

/**
 * Map based implementation of {@link S3SecretStore}.
 */
public class S3SecretStoreMap implements S3SecretStore {
  private final Map<String, S3SecretValue> map = new ConcurrentHashMap<>();

  public S3SecretStoreMap(Map<String, S3SecretValue> map) {
    this.map.putAll(map);
  }

  @Override
  public void storeSecret(String kerberosId, S3SecretValue secret)
      throws IOException {
    map.put(kerberosId, secret);
  }

  @Override
  public S3SecretValue getSecret(String kerberosID) throws IOException {
    return map.get(kerberosID);
  }

  @Override
  public void revokeSecret(String kerberosId) throws IOException {
    map.remove(kerberosId);
  }

  @Override
  public S3Batcher batcher() {
    return null;
  }
}

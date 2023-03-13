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

/**
 * Configuration keys for S3 secret store.
 */
public final class S3SecretStoreConfigurationKeys {
  public static final String S3_SECRET_STORAGE_TYPE = "ozone.secret.s3.store";
  public static final String DEFAULT_SECRET_STORAGE_TYPE
      = S3SecretStoreType.ROCKSDB.name();

  //Vault configs
  public static final String PREFIX = "ozone.secret.s3.store.vault.";

  public static final String ADDRESS = PREFIX + "address";
  public static final String NAMESPACE = PREFIX + "namespace";
  public static final String SECRET_PATH = PREFIX + "secretpath";

  public static final String AUTH_TYPE = PREFIX + "auth";

  public static final String TOKEN = AUTH_TYPE + "token";

  public static final String APP_ROLE_ID = AUTH_TYPE + "approle.id";
  public static final String APP_ROLE_SECRET = AUTH_TYPE + "approle.secret";
  public static final String APP_ROLE_PATH = AUTH_TYPE + "approle.path";

  public static final String ENGINE_VER = PREFIX + "enginever";

  //SSL configs
  public static final String TRUST_STORE_TYPE = PREFIX + "trust.store.type";
  public static final String TRUST_STORE_PATH = PREFIX + "trust.store.path";
  public static final String TRUST_STORE_PASSWORD
      = PREFIX + "trust.store.password";

  public static final String KEY_STORE_TYPE = PREFIX + "key.store.type";
  public static final String KEY_STORE_PATH = PREFIX + "key.store.path";
  public static final String KEY_STORE_PASSWORD = PREFIX + "key.store.password";

  //Cache configuration
  public static final String CACHE_PREFIX = PREFIX + "cache";

  public static final String CACHE_LIFETIME = CACHE_PREFIX + "expireTime";
  public static final long DEFAULT_CACHE_LIFETIME = 600;

  public static final String CACHE_MAX_SIZE = CACHE_PREFIX + "capacity";
  public static final long DEFAULT_CACHE_MAX_SIZE = Long.MAX_VALUE;

  /**
   * Never constructed.
   */
  private S3SecretStoreConfigurationKeys() {

  }

}

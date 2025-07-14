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

package org.apache.hadoop.ozone.s3.remote;

/**
 * Configuration keys for S3 secret store.
 */
public final class S3SecretRemoteStoreConfigurationKeys {
  //Vault configs
  public static final String PREFIX = "ozone.secret.s3.store.remote.vault.";

  public static final String ADDRESS = PREFIX + "address";
  public static final String NAMESPACE = PREFIX + "namespace";
  public static final String SECRET_PATH = PREFIX + "secretpath";

  public static final String AUTH_TYPE = PREFIX + "auth";
  public static final String AUTH_PREFIX = AUTH_TYPE + ".";

  public static final String TOKEN = AUTH_PREFIX + "token";

  public static final String APP_ROLE_ID = AUTH_PREFIX + "approle.id";
  public static final String APP_ROLE_SECRET = AUTH_PREFIX + "approle.secret";
  public static final String APP_ROLE_PATH = AUTH_PREFIX + "approle.path";

  public static final String ENGINE_VER = PREFIX + "enginever";

  //SSL configs
  public static final String TRUST_STORE_TYPE = PREFIX + "trust.store.type";
  public static final String TRUST_STORE_PATH = PREFIX + "trust.store.path";
  public static final String TRUST_STORE_PASSWORD
      = PREFIX + "trust.store.password";

  public static final String KEY_STORE_TYPE = PREFIX + "key.store.type";
  public static final String KEY_STORE_PATH = PREFIX + "key.store.path";
  public static final String KEY_STORE_PASSWORD = PREFIX + "key.store.password";

  /**
   * Never constructed.
   */
  private S3SecretRemoteStoreConfigurationKeys() {

  }

}

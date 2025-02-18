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

package org.apache.hadoop.ozone.om.s3;

/**
 * Configuration keys for S3 secret store and cache.
 */
public final class S3SecretStoreConfigurationKeys {
  private static final String PREFIX = "ozone.secret.s3.store.";

  public static final String S3_SECRET_STORAGE_TYPE = PREFIX + "provider";
  public static final Class<LocalS3StoreProvider> DEFAULT_SECRET_STORAGE_TYPE
            = LocalS3StoreProvider.class;

  //Cache configuration
  public static final String CACHE_PREFIX = PREFIX + "cache.";

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

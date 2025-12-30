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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.om.S3InMemoryCache;
import org.apache.hadoop.ozone.om.S3SecretCache;

/**
 * Provider of {@link S3SecretCache}.
 */
public interface S3SecretCacheProvider {
  /**
   * In-memory cache implementation.
   */
  S3SecretCacheProvider IN_MEMORY = conf -> new S3InMemoryCache();

  /**
   * Returns S3 secret cache instance constructed by provided configuration.
   *
   * @param conf {@link Configuration} of Ozone.
   * @return S3 secret cache instance.
   */
  S3SecretCache get(Configuration conf);
}

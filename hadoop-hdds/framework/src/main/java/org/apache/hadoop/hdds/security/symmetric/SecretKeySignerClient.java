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

package org.apache.hadoop.hdds.security.symmetric;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;

/**
 * Define the client-side API that the token signers (like OM) uses to retrieve
 * the secret key to sign data.
 */
public interface SecretKeySignerClient {
  ManagedSecretKey getCurrentSecretKey();

  /**
   * This is where the actual implementation can prefetch the current
   * secret key or initialize ay necessary resources, e.g. cache or executors.
   */
  default void start(ConfigurationSource conf) throws IOException {
  }

  /**
   * Give a chance for the implementation to clean up acquired resources.
   */
  default void stop() {
  }

  default void refetchSecretKey() {
  }
}

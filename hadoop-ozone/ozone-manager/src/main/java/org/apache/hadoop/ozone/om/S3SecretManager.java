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

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface to manager s3 secret.
 */
public interface S3SecretManager {
  Logger LOG = LoggerFactory.getLogger(S3SecretManager.class);

  /**
   * API to get s3 secret for given kerberos identifier.
   * @param kerberosID kerberos principal.
   * @return associated s3 secret or null if secret not existed.
   * @throws IOException
   */
  S3SecretValue getSecret(String kerberosID) throws IOException;

  /**
   * API to get s3 secret for given awsAccessKey.
   * @param awsAccessKey aws access key.
   * */
  String getSecretString(String awsAccessKey) throws IOException;

  /**
   * Store provided s3 secret and associate it with kerberos principal.
   * @param kerberosId kerberos principal.
   * @param secretValue s3 secret value.
   * @throws IOException in case when secret storing failed.
   */
  void storeSecret(String kerberosId, S3SecretValue secretValue)
      throws IOException;

  /**
   * Revoke s3 secret which associated with provided kerberos principal.
   * @param kerberosId kerberos principal.
   * @throws IOException in case when revoke failed.
   */
  void revokeSecret(String kerberosId) throws IOException;

  /**
   * Clear s3 secret cache when double buffer is flushed to the DB.
   */
  void clearS3Cache(List<Long> epochs);

  /**
   * Apply provided action under write lock.
   * @param lockId lock identifier.
   * @param action custom action.
   * @param <T> type of action result.
   * @return action result.
   * @throws IOException in case when action failed.
   */
  <T> T doUnderLock(String lockId, S3SecretFunction<T> action)
      throws IOException;

  /**
   * Default implementation of secret check method.
   * @param kerberosId kerberos principal.
   * @return true if exist associated s3 secret for given {@code kerberosId},
   * false if not.
   */
  default boolean hasS3Secret(String kerberosId) throws IOException {
    return getSecret(kerberosId) != null;
  }

  S3Batcher batcher();

  default boolean isBatchSupported() {
    return batcher() != null;
  }

  /**
   * Direct cache accessor.
   * @return s3 secret cache.
   */
  S3SecretCache cache();

  default void updateCache(String accessId, S3SecretValue secret) {
    S3SecretCache cache = cache();
    if (cache != null) {
      LOG.info("Updating cache for accessId/user: {}.", accessId);
      cache.put(accessId, secret);
    }
  }

  default void invalidateCacheEntry(String id) {
    S3SecretCache cache = cache();
    if (cache != null) {
      cache.invalidate(id);
    }
  }

  default void clearCache(List<Long> flushedTransactionIds) {
    S3SecretCache cache = cache();
    if (cache != null) {
      cache.clearCache(flushedTransactionIds);
    }
  }

}

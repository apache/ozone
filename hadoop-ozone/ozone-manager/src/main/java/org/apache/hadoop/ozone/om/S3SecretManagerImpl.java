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

import static org.apache.hadoop.hdds.security.exception.OzoneSecurityException.ResultCodes.S3_SECRET_NOT_FOUND;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.security.exception.OzoneSecurityException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3 Secret manager.
 */
public class S3SecretManagerImpl implements S3SecretManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(S3SecretManagerImpl.class);

  private final S3SecretStore s3SecretStore;
  private final S3SecretCache s3SecretCache;

  /**
   * Constructs S3SecretManager.
   * @param s3SecretStore s3 secret store.
   * @param s3SecretCache s3 secret cache.
   */
  public S3SecretManagerImpl(S3SecretStore s3SecretStore,
                             S3SecretCache s3SecretCache) {
    this.s3SecretStore = s3SecretStore;
    this.s3SecretCache = s3SecretCache;
  }

  @Override
  public S3SecretValue getSecret(String kerberosID) throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(kerberosID),
        "kerberosID cannot be null or empty.");
    S3SecretValue cacheValue = s3SecretCache.get(kerberosID);
    if (cacheValue != null) {
      if (cacheValue.isDeleted()) {
        // The cache entry is marked as deleted which means the user has
        // purposely deleted the secret. Hence, we do not have to check the DB.
        return null;
      }
      return cacheValue;
    }
    S3SecretValue result = s3SecretStore.getSecret(kerberosID);
    if (result != null) {
      updateCache(kerberosID, result);
    }
    return result;
  }

  @Override
  public String getSecretString(String awsAccessKey)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotBlank(awsAccessKey),
        "awsAccessKeyId cannot be null or empty.");
    LOG.trace("Get secret for awsAccessKey:{}", awsAccessKey);

    S3SecretValue cacheValue = s3SecretCache.get(awsAccessKey);
    if (cacheValue != null) {
      return cacheValue.getAwsSecret();
    }
    S3SecretValue s3Secret = s3SecretStore.getSecret(awsAccessKey);
    if (s3Secret == null) {
      throw new OzoneSecurityException("S3 secret not found for " +
          "awsAccessKeyId " + awsAccessKey, S3_SECRET_NOT_FOUND);
    }
    updateCache(awsAccessKey, s3Secret);
    return s3Secret.getAwsSecret();
  }

  @Override
  public void storeSecret(String kerberosId, S3SecretValue secretValue)
      throws IOException {
    s3SecretStore.storeSecret(kerberosId, secretValue);
    updateCache(kerberosId, secretValue);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Secret for accessKey:{} stored", kerberosId);
    }
  }

  @Override
  public void revokeSecret(String kerberosId) throws IOException {
    s3SecretStore.revokeSecret(kerberosId);
    invalidateCacheEntry(kerberosId);
  }

  @Override
  public void clearS3Cache(List<Long> flushedTransactionIds) {
    clearCache(flushedTransactionIds);
  }

  @Override
  public <T> T doUnderLock(String lockId, S3SecretFunction<T> action)
      throws IOException {
    throw new UnsupportedOperationException(
        "Lock on locked secret manager is not supported.");
  }

  @Override
  public S3SecretCache cache() {
    return s3SecretCache;
  }

  @Override
  public S3Batcher batcher() {
    return s3SecretStore.batcher();
  }

  @Override
  public void updateCache(String kerberosID, S3SecretValue secret) {
    S3SecretManager.super.updateCache(kerberosID, secret);
  }
}

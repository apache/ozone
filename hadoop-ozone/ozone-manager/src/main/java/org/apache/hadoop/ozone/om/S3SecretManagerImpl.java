/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.hdds.security.OzoneSecurityException;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hdds.security.OzoneSecurityException.ResultCodes.S3_SECRET_NOT_FOUND;

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
    Preconditions.checkArgument(Strings.isNotBlank(kerberosID),
        "kerberosID cannot be null or empty.");
    return s3SecretStore.getSecret(kerberosID);
  }

  @Override
  public String getSecretString(String awsAccessKey)
      throws IOException {
    Preconditions.checkArgument(Strings.isNotBlank(awsAccessKey),
        "awsAccessKeyId cannot be null or empty.");
    LOG.trace("Get secret for awsAccessKey:{}", awsAccessKey);

    S3SecretValue s3Secret = s3SecretStore.getSecret(awsAccessKey);
    if (s3Secret == null) {
      throw new OzoneSecurityException("S3 secret not found for " +
          "awsAccessKeyId " + awsAccessKey, S3_SECRET_NOT_FOUND);
    }

    return s3Secret.getAwsSecret();
  }

  @Override
  public void storeSecret(String kerberosId, S3SecretValue secretValue)
      throws IOException {
    s3SecretStore.storeSecret(kerberosId, secretValue);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Secret for accessKey:{} stored", kerberosId);
    }
  }

  @Override
  public void revokeSecret(String kerberosId) throws IOException {
    s3SecretStore.revokeSecret(kerberosId);
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
}

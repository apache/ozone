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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.S3_SECRET_LOCK;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;

/**
 * Wrapper with lock logic of {@link S3SecretManager}.
 */
public class S3SecretLockedManager implements S3SecretManager {
  private final S3SecretManager secretManager;
  private final IOzoneManagerLock lock;

  public S3SecretLockedManager(S3SecretManager secretManager,
                               IOzoneManagerLock lock) {
    this.secretManager = secretManager;
    this.lock = lock;
  }

  @Override
  public S3SecretValue getSecret(String kerberosID) throws IOException {
    lock.acquireWriteLock(S3_SECRET_LOCK, kerberosID);
    try {
      return secretManager.getSecret(kerberosID);
    } finally {
      lock.releaseWriteLock(S3_SECRET_LOCK, kerberosID);
    }
  }

  @Override
  public String getSecretString(String awsAccessKey) throws IOException {
    lock.acquireReadLock(S3_SECRET_LOCK, awsAccessKey);
    try {
      return secretManager.getSecretString(awsAccessKey);
    } finally {
      lock.releaseReadLock(S3_SECRET_LOCK, awsAccessKey);
    }
  }

  @Override
  public void storeSecret(String kerberosId, S3SecretValue secretValue)
      throws IOException {
    lock.acquireWriteLock(S3_SECRET_LOCK, kerberosId);
    try {
      secretManager.storeSecret(kerberosId, secretValue);
    } finally {
      lock.releaseWriteLock(S3_SECRET_LOCK, kerberosId);
    }
  }

  @Override
  public void revokeSecret(String kerberosId) throws IOException {
    lock.acquireWriteLock(S3_SECRET_LOCK, kerberosId);
    try {
      secretManager.revokeSecret(kerberosId);
    } finally {
      lock.releaseWriteLock(S3_SECRET_LOCK, kerberosId);
    }
  }

  @Override
  public void clearS3Cache(List<Long> epochs) {
    lock.acquireWriteLock(S3_SECRET_LOCK, "cache");
    try {
      secretManager.clearS3Cache(epochs);
    } finally {
      lock.releaseWriteLock(S3_SECRET_LOCK, "cache");
    }
  }

  @Override
  public <T> T doUnderLock(String lockId, S3SecretFunction<T> action)
      throws IOException {
    lock.acquireWriteLock(S3_SECRET_LOCK, lockId);
    try {
      return action.accept(secretManager);
    } finally {
      lock.releaseWriteLock(S3_SECRET_LOCK, lockId);
    }
  }

  @Override
  public S3Batcher batcher() {
    return secretManager.batcher();
  }

  @Override
  public S3SecretCache cache() {
    return secretManager.cache();
  }
}

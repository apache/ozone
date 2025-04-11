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

import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link SecretKeyState}.
 */
public final class SecretKeyStateImpl implements SecretKeyState {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecretKeyStateImpl.class);

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private List<ManagedSecretKey> sortedKeys;
  private ManagedSecretKey currentKey;
  private Map<UUID, ManagedSecretKey> keyById;

  private final SecretKeyStore keyStore;

  /**
   * Instantiate a state with no keys. This state object needs to be backed by
   * a proper replication proxy so that the @Replication method works.
   */
  public SecretKeyStateImpl(SecretKeyStore keyStore) {
    this.keyStore = requireNonNull(keyStore);
  }

  /**
   * Get the current active key, which is used for signing tokens. This is
   * also the latest key managed by this state.
   */
  @Override
  public ManagedSecretKey getCurrentKey() {
    lock.readLock().lock();
    try {
      return currentKey;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public ManagedSecretKey getKey(UUID id) {
    lock.readLock().lock();
    try {
      // Return null if not initialized yet.
      if (keyById == null) {
        return null;
      }
      return keyById.get(id);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the keys that managed by this manager.
   * The returned keys are sorted by creation time, in the order of latest
   * to oldest.
   */
  @Override
  public List<ManagedSecretKey> getSortedKeys() {
    lock.readLock().lock();
    try {
      return sortedKeys;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Update the SecretKeys.
   * This method replicates SecretKeys across all SCM instances.
   */
  @Override
  public void updateKeys(List<ManagedSecretKey> newKeys) {
    updateKeysInternal(newKeys);
  }

  private void updateKeysInternal(List<ManagedSecretKey> newKeys) {
    LOG.info("Updating keys with {}", newKeys);
    lock.writeLock().lock();
    try {
      // Store sorted keys in order of latest to oldest and make it
      // immutable so that can be used to answer queries directly.
      sortedKeys = Collections.unmodifiableList(
          newKeys.stream()
              .sorted(comparing(ManagedSecretKey::getCreationTime).reversed())
              .collect(toList())
      );
      currentKey = sortedKeys.get(0);
      keyById = newKeys.stream().collect(toMap(
          ManagedSecretKey::getId,
          Function.identity()
      ));
      LOG.info("Current key updated {}", currentKey);
      keyStore.save(sortedKeys);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void reinitialize(List<ManagedSecretKey> secretKeys) {
    updateKeysInternal(secretKeys);
  }
}

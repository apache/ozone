/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.security.symmetric;

import javax.annotation.Nonnull;
import javax.crypto.KeyGenerator;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.time.Duration.between;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class SecretKeyManagerImpl implements SecretKeyManager {
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private ManagedSecretKey currentKey;
  private final Map<UUID, ManagedSecretKey> allKeys = new HashMap<>();

  private final SecretKeyStore keyStore;
  private final Duration validityDuration;
  private final Duration rotationDuration;

  private final KeyGenerator keyGenerator;

  SecretKeyManagerImpl(SecretKeyStore keyStore, Duration validityDuration,
                       Duration rotationDuration, String algorithm) {
    this.keyStore = keyStore;
    this.validityDuration = requireNonNull(validityDuration);
    this.rotationDuration = requireNonNull(rotationDuration);
    this.keyGenerator = createKeyGenerator(algorithm);

    reloadKeys();
  }

  private void reloadKeys() {
    List<ManagedSecretKey> sortedKeys = keyStore.load()
        .stream()
        .filter(x -> !x.isExpired())
        .sorted(comparing(ManagedSecretKey::getCreationTime))
        .collect(Collectors.toList());

    if (sortedKeys.isEmpty()) {
      // First start, generate new key as the current key.
      currentKey = generateSecretKey();
      allKeys.put(currentKey.getId(), currentKey);
    } else {
      // For restarts, reload allKeys and take the latest one as current.
      currentKey = sortedKeys.get(sortedKeys.size() - 1);
      sortedKeys.forEach(x -> allKeys.put(x.getId(), x));
    }
  }

  @Override
  @Nonnull
  public ManagedSecretKey getCurrentKey() {
    lock.readLock().lock();
    try {
      return requireNonNull(currentKey);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Set<ManagedSecretKey> getAllKeys() {
    lock.readLock().lock();
    try {
      return new HashSet<>(allKeys.values());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean checkAndRotate() {
    ManagedSecretKey newCurrentKey = null;
    Set<ManagedSecretKey> updatedKeys = null;

    lock.readLock().lock();
    try {
      if (shouldRotate()) {
        newCurrentKey = generateSecretKey();
        updatedKeys = allKeys.values()
            .stream().filter(x -> !x.isExpired())
            .collect(Collectors.toSet());
        updatedKeys.add(newCurrentKey);
      }
    } finally {
      lock.readLock().unlock();
    }

    if (newCurrentKey != null) {
      updateKeys(newCurrentKey, updatedKeys);
      return true;
    }

    return false;
  }

  @Override
  public void updateKeys(ManagedSecretKey newCurrentKey,
                         Set<ManagedSecretKey> newAllKeys) {
    lock.writeLock().lock();
    try {
      currentKey = newCurrentKey;
      allKeys.clear();
      for (ManagedSecretKey secretKey : newAllKeys) {
        allKeys.put(secretKey.getId(), secretKey);
      }
      keyStore.save(allKeys.values());
    } finally {
      lock.writeLock().unlock();
    }
  }

  private boolean shouldRotate() {
    Duration established = between(currentKey.getCreationTime(), Instant.now());
    return established.compareTo(rotationDuration) >= 0;
  }

  private KeyGenerator createKeyGenerator(String algorithm) {
    try {
      return KeyGenerator.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException("Error creating KeyGenerator for " +
          "algorithm " + algorithm, e);
    }
  }

  private ManagedSecretKey generateSecretKey() {
    Instant now = Instant.now();
    return new ManagedSecretKey(
        UUID.randomUUID(),
        now,
        now.plus(validityDuration),
        keyGenerator.generateKey()
    );
  }

}

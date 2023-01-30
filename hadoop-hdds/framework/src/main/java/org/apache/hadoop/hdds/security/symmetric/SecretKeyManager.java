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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static java.time.Duration.between;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * This component manages symmetric SecretKey life-cycle, including generation,
 * rotation and destruction.
 */
public class SecretKeyManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecretKeyManager.class);

  private final SecretKeyState state;
  private boolean pendingInititializedState = false;
  private final Duration rotationDuration;
  private final Duration validityDuration;
  private final SecretKeyStore keyStore;

  private final KeyGenerator keyGenerator;

  public SecretKeyManager(SecretKeyState state,
                          SecretKeyStore keyStore,
                          Duration rotationDuration,
                          Duration validityDuration,
                          String algorithm) {
    this.state = requireNonNull(state);
    this.rotationDuration = requireNonNull(rotationDuration);
    this.validityDuration = requireNonNull(validityDuration);
    this.keyStore = requireNonNull(keyStore);
    this.keyGenerator = createKeyGenerator(algorithm);
  }

  public SecretKeyManager(SecretKeyState state,
                          SecretKeyStore keyStore,
                          SecretKeyConfig config) {
    this(state, keyStore, config.getRotateDuration(),
        config.getExpiryDuration(), config.getAlgorithm());
  }

  /**
   * Initialize the state from by loading SecretKeys from local file, or
   * generate new keys if the file doesn't exist.
   */
  public synchronized boolean initialize() {
    if (state.getCurrentKey() != null) {
      return false;
    }

    // Load and filter expired keys.
    List<ManagedSecretKey> allKeys = keyStore.load()
        .stream()
        .filter(x -> !x.isExpired())
        .collect(toList());

    if (allKeys.isEmpty()) {
      // if no valid key present , generate new key as the current key.
      // This happens at first start or restart after being down for
      // a significant time.
      ManagedSecretKey newKey = generateSecretKey();
      allKeys.add(newKey);
      LOG.info("No valid keys has been loaded, " +
          "a new key is generated: {}", newKey);
    } else {
      LOG.info("Keys reloaded: {}", allKeys);
    }

    // First, update the SecretKey state to make it visible immediately on the
    // current instance.
    state.updateKeysInternal(allKeys);
    // Then, remember to replicate SecretKey states to all instances.
    pendingInititializedState = true;
    return true;
  }

  public synchronized void flushInitializedState() throws TimeoutException {
    if (pendingInititializedState) {
      LOG.info("Replicating initialized state.");
      state.updateKeys(state.getSortedKeys());
      pendingInititializedState = false;
    }
  }

  /**
   * Check and rotate the keys.
   *
   * @return true if rotation actually happens, false if it doesn't.
   */
  public synchronized boolean checkAndRotate() throws TimeoutException {
    flushInitializedState();

    ManagedSecretKey currentKey = state.getCurrentKey();
    if (shouldRotate(currentKey)) {
      ManagedSecretKey newCurrentKey = generateSecretKey();
      List<ManagedSecretKey> updatedKeys = state.getSortedKeys()
          .stream().filter(x -> !x.isExpired())
          .collect(toList());
      updatedKeys.add(newCurrentKey);

      LOG.info("SecretKey rotation is happening, new key generated {}",
          newCurrentKey);
      state.updateKeys(updatedKeys);
      return true;
    }
    return false;
  }

  private boolean shouldRotate(ManagedSecretKey currentKey) {
    Duration established = between(currentKey.getCreationTime(), Instant.now());
    return established.compareTo(rotationDuration) >= 0;
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

  private KeyGenerator createKeyGenerator(String algorithm) {
    try {
      return KeyGenerator.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException("Error creating KeyGenerator for " +
          "algorithm " + algorithm, e);
    }
  }
}

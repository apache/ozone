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

import static java.time.Duration.between;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This component manages symmetric SecretKey life-cycle, including generation,
 * rotation and destruction.
 */
public class SecretKeyManager implements SecretKeyClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecretKeyManager.class);

  private final SecretKeyState state;
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
   * If the SecretKey state is not initialized, initialize it from by loading
   * SecretKeys from local file, or generate new keys if the file doesn't
   * exist.
   */
  public synchronized void checkAndInitialize() throws SCMException {
    if (isInitialized()) {
      return;
    }

    LOG.info("Initializing SecretKeys.");

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
      LOG.info("No valid key has been loaded. " +
          "A new key is generated: {}", newKey);
    } else {
      LOG.info("Keys reloaded: {}", allKeys);
    }

    state.updateKeys(allKeys);
  }

  public boolean isInitialized() {
    return state.getCurrentKey() != null;
  }

  /**
   * Check and rotate the keys.
   *
   * @return true if rotation actually happens, false if it doesn't.
   */
  public synchronized boolean checkAndRotate(boolean force)
      throws SCMException {
    // Initialize the state if it's not initialized already.
    checkAndInitialize();

    ManagedSecretKey currentKey = state.getCurrentKey();
    if (force || shouldRotate(currentKey)) {
      ManagedSecretKey newCurrentKey = generateSecretKey();
      List<ManagedSecretKey> updatedKeys = state.getSortedKeys()
          .stream().filter(x -> !x.isExpired())
          .collect(toList());
      updatedKeys.add(newCurrentKey);

      LOG.info((force ? "Forced " : "") +
              "SecretKey rotation is happening, new key generated {}",
          newCurrentKey);
      state.updateKeys(updatedKeys);
      return true;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "The latest key was created at: " + currentKey.getCreationTime() +
              " which does not pass the rotation duration");
    }
    return false;
  }

  @Override
  public ManagedSecretKey getCurrentSecretKey() {
    return state.getCurrentKey();
  }

  @Override
  public ManagedSecretKey getSecretKey(UUID id) {
    return state.getKey(id);
  }

  public List<ManagedSecretKey> getSortedKeys() {
    return state.getSortedKeys();
  }

  public void reinitialize(List<ManagedSecretKey> secretKeys) {
    state.reinitialize(secretKeys);
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

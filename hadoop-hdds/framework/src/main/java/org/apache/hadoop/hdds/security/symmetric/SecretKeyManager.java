package org.apache.hadoop.hdds.security.symmetric;

import org.apache.hadoop.hdds.HddsUtils;
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
  private static final Logger LOG = LoggerFactory.getLogger(SecretKeyManager.class);

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
   * Initialize the state from by loading SecretKeys from local file, or
   * generate new keys if the file doesn't exist.
   *
   * @throws TimeoutException can possibly occur when replicating the state.
   */
  public synchronized void initialize() throws TimeoutException {
    if (state.getCurrentKey() != null) {
      return;
    }

    List<ManagedSecretKey> sortedKeys = keyStore.load()
        .stream()
        .filter(x -> !x.isExpired())
        .sorted(comparing(ManagedSecretKey::getCreationTime))
        .collect(toList());

    ManagedSecretKey currentKey;
    if (sortedKeys.isEmpty()) {
      // First start, generate new key as the current key.
      currentKey = generateSecretKey();
      sortedKeys.add(currentKey);
      LOG.info("No keys is loaded, generated new key: {}", currentKey);
    } else {
      // For restarts, reload allKeys and take the latest one as current.
      currentKey = sortedKeys.get(sortedKeys.size() - 1);
      LOG.info("Key reloaded, current key: {}, all keys: {}", currentKey,
          sortedKeys);
    }

    state.updateKeys(currentKey, sortedKeys);
  }

  /**
   * Check and rotate the keys.
   *
   * @return true if rotation actually happens, false if it doesn't.
   */
  public synchronized boolean checkAndRotate() throws TimeoutException {
    ManagedSecretKey newCurrentKey = null;
    List<ManagedSecretKey> updatedKeys = null;

    ManagedSecretKey currentKey = state.getCurrentKey();
    if (shouldRotate(currentKey)) {
      newCurrentKey = generateSecretKey();
      updatedKeys = state.getAllKeys()
          .stream().filter(x -> !x.isExpired())
          .collect(toList());
      updatedKeys.add(newCurrentKey);
    }

    if (newCurrentKey != null) {
      LOG.info("SecretKey rotation is happening, new key generated {}",
          newCurrentKey);
      state.updateKeys(newCurrentKey, updatedKeys);
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

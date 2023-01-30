package org.apache.hadoop.hdds.security.symmetric;

import org.apache.hadoop.hdds.scm.metadata.Replicate;

import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * This component holds the state of managed SecretKeys, including the
 * current key and all active keys.
 */
public interface SecretKeyState {
  /**
   * Get the current active key, which is used for signing tokens. This is
   * also the latest key managed by this state.
   */
  ManagedSecretKey getCurrentKey();

  /**
   * Get the keys that managed by this manager.
   * The returned keys are sorted by creation time, in the order of latest
   * to oldest.
   */
  List<ManagedSecretKey> getSortedKeys();

  /**
   * Update the SecretKeys.
   * This method replicates SecretKeys across all SCM instances.
   */
  @Replicate
  void updateKeys(List<ManagedSecretKey> newKeys) throws TimeoutException;

  /**
   * Update the SecretKeys on this instance only.
   */
  void updateKeysInternal(List<ManagedSecretKey> newKeys);
}

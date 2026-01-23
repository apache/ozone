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

import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.metadata.Replicate;

/**
 * This component holds the state of managed SecretKeys, including the
 * current key and all active keys.
 */
public interface SecretKeyState {
  /**
   * Get the current active key, which is used for signing tokens. This is
   * also the latest key managed by this state.
   *
   * @return the current active key, or null if the state is not initialized.
   */
  ManagedSecretKey getCurrentKey();

  ManagedSecretKey getKey(UUID id);

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
  void updateKeys(List<ManagedSecretKey> newKeys) throws SCMException;

  /**
   * Update SecretKeys from a snapshot from SCM leader.
   */
  void reinitialize(List<ManagedSecretKey> secretKeys);
}

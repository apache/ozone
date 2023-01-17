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

import org.apache.hadoop.hdds.scm.metadata.Replicate;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * This component holds the state of managed SecretKeys, including the
 * current key and all active keys.
 */
public interface SecretKeyState {
  /**
   * @return the current active key, which is used for signing tokens.
   */
  ManagedSecretKey getCurrentKey();

  /**
   * @return all the keys that managed by this manager.
   */
  Set<ManagedSecretKey> getAllKeys();

  /**
   * Update the SecretKeys.
   * This is a short-hand for replicating SecretKeys across all SCM instances
   * after each rotation.
   */
  @Replicate
  void updateKeys(ManagedSecretKey currentKey,
                  List<ManagedSecretKey> allKeys) throws TimeoutException;
}

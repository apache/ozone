/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.server.upgrade;

import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.utils.db.Table;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Manages the state of finalization in SCM.
 */
public interface FinalizationStateManager {

  @Replicate
  void addFinalizingMark() throws IOException, TimeoutException;

  @Replicate
  void removeFinalizingMark() throws IOException, TimeoutException;

  @Replicate
  void finalizeLayoutFeature(Integer layoutVersion)
      throws IOException, TimeoutException;

  /**
   * @param query The checkpoint to check for being crossed.
   * @return true if SCM's disk state indicates this checkpoint has been
   * crossed. False otherwise.
   */
  boolean crossedCheckpoint(FinalizationCheckpoint query);

  FinalizationCheckpoint getFinalizationCheckpoint();

  void setUpgradeContext(SCMUpgradeFinalizationContext context);

  /**
   * Called on snapshot installation.
   */
  void reinitialize(Table<String, String> newFinalizationStore)
      throws IOException;
}

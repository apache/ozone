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
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * Manages the state of finalization in SCM.
 */
public interface FinalizationManager {

  UpgradeFinalizer.StatusAndMessages finalizeUpgrade(String upgradeClientID)
      throws IOException;

  UpgradeFinalizer.StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean takeover, boolean readonly
  ) throws IOException;

  BasicUpgradeFinalizer<SCMUpgradeFinalizer.SCMUpgradeFinalizationContext, HDDSLayoutVersionManager> getUpgradeFinalizer();

  void runPrefinalizeStateActions() throws IOException;

  boolean passedCheckpoint(FinalizationStateManager.FinalizationCheckpoint checkpoint);
}

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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;

import java.io.IOException;

/**
 * Class to initiate SCM finalization and query its progress.
 */
public interface FinalizationManager {

  UpgradeFinalizer.StatusAndMessages finalizeUpgrade(String upgradeClientID)
      throws IOException;

  UpgradeFinalizer.StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean takeover, boolean readonly
  ) throws IOException;

  @VisibleForTesting
  BasicUpgradeFinalizer<SCMUpgradeFinalizationContext, HDDSLayoutVersionManager>
      getUpgradeFinalizer();

  void runPrefinalizeStateActions() throws IOException;

  boolean crossedCheckpoint(FinalizationCheckpoint checkpoint);

  FinalizationCheckpoint getCheckpoint();

  void buildUpgradeContext(NodeManager nodeManager,
                                  PipelineManager pipelineManager,
                                  SCMContext scmContext);

  void reinitialize(Table<String, String> finalizationStore) throws IOException;

  void onLeaderReady();

  static boolean shouldCreateNewPipelines(FinalizationCheckpoint checkpoint) {
    return !checkpoint.hasCrossed(FinalizationCheckpoint.FINALIZATION_STARTED)
        || checkpoint.hasCrossed(FinalizationCheckpoint.MLV_EQUALS_SLV);
  }

  static boolean shouldTellDatanodesToFinalize(
      FinalizationCheckpoint checkpoint) {
    return checkpoint.hasCrossed(FinalizationCheckpoint.MLV_EQUALS_SLV);
  }

}

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

package org.apache.hadoop.ozone.container.upgrade;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.upgrade.DatanodeUpgradeAction;
import org.apache.hadoop.hdds.upgrade.DatanodeUpgradeActionProvider;
import org.apache.hadoop.hdds.upgrade.HDDSVersionUtils;
import org.apache.hadoop.ozone.container.common.DatanodeStorage;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

/**
 * Datanode-specific version manager that wires upgrade actions internally.
 */
public class DatanodeVersionManager extends ComponentVersionManager {

  private final DatanodeStorage storage;
  private final Map<ComponentVersion, DatanodeUpgradeAction> upgradeActions;
  private final DatanodeStateMachine upgradeActionArg;

  public DatanodeVersionManager(DatanodeStorage storage, DatanodeStateMachine upgradeActionArg) throws IOException {
    this(storage, upgradeActionArg, new DatanodeUpgradeActionProvider());
  }

  @VisibleForTesting
  public DatanodeVersionManager(DatanodeStorage storage, DatanodeStateMachine upgradeActionArg,
      ComponentUpgradeActionProvider<DatanodeUpgradeAction> upgradeActionProvider) throws IOException {
    super(HDDSVersionUtils.deserializedPersistedApparentVersion(storage.getApparentVersion()),
        HDDSVersion.SOFTWARE_VERSION);
    this.storage = storage;
    this.upgradeActionArg = upgradeActionArg;
    upgradeActions = upgradeActionProvider.load();
  }

  @Override
  protected void persistApparentVersion(ComponentVersion newVersion) throws IOException {
    storage.setApparentVersion(newVersion.serialize());
    storage.persistCurrentState();
  }

  @Override
  public int getPersistedApparentVersion() {
    return storage.getApparentVersion();
  }

  @VisibleForTesting
  public Map<ComponentVersion, DatanodeUpgradeAction> getUpgradeActionsForTesting() {
    return upgradeActions;
  }

  @Override
  protected void runUpgradeAction(ComponentVersion version) throws UpgradeException {
    DatanodeUpgradeAction action = upgradeActions.get(version);
    if (action == null) {
      return;
    }
    try {
      action.execute(upgradeActionArg);
    } catch (Exception e) {
      logAndThrow(e, "Datanode upgrade action for version " + version + " failed.",
          UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED);
    }
  }
}

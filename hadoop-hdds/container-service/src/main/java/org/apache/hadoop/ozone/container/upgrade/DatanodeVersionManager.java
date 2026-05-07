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
import org.apache.hadoop.hdds.upgrade.DatanodeUpgradeAction;
import org.apache.hadoop.hdds.upgrade.DatanodeUpgradeActionProvider;
import org.apache.hadoop.hdds.upgrade.HDDSVersionManager;
import org.apache.hadoop.ozone.container.common.DatanodeStorage;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

/**
 * Datanode-specific version manager that wires upgrade actions internally.
 */
public class DatanodeVersionManager extends HDDSVersionManager {

  private final Map<ComponentVersion, DatanodeUpgradeAction> upgradeActions;
  private final DatanodeStateMachine upgradeActionArg;

  public DatanodeVersionManager(DatanodeStorage storage, DatanodeStateMachine upgradeActionArg) throws IOException {
    super(storage);
    upgradeActions = new DatanodeUpgradeActionProvider().load();
    this.upgradeActionArg = upgradeActionArg;
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

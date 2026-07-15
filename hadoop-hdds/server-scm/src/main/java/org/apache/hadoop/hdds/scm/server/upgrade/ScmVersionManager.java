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

package org.apache.hadoop.hdds.scm.server.upgrade;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.upgrade.HDDSVersionUtils;
import org.apache.hadoop.hdds.upgrade.ScmUpgradeAction;
import org.apache.hadoop.hdds.upgrade.ScmUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.RatisBasedVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

/**
 * SCM-specific version manager that wires upgrade actions internally.
 */
public class ScmVersionManager extends RatisBasedVersionManager {

  private final SCMStorageConfig storage;
  private final Map<ComponentVersion, ScmUpgradeAction> upgradeActions;
  private final OzoneStorageContainerManager upgradeActionArg;

  public ScmVersionManager(SCMStorageConfig storage, OzoneStorageContainerManager upgradeActionArg) throws IOException {
    this(storage, upgradeActionArg, new ScmUpgradeActionProvider());
  }

  // Used by Recon's node manager to track Datanode versions without running SCM specific upgrade actions.
  public ScmVersionManager(SCMStorageConfig storage,
      OzoneStorageContainerManager upgradeActionArg,
      ComponentUpgradeActionProvider<ScmUpgradeAction> upgradeActionProvider)
      throws IOException {
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
  public Map<ComponentVersion, ScmUpgradeAction> getUpgradeActionsForTesting() {
    return upgradeActions;
  }

  @Override
  protected void runUpgradeAction(ComponentVersion version) throws UpgradeException {
    ScmUpgradeAction action = upgradeActions.get(version);
    if (action == null) {
      return;
    }
    try {
      action.execute(upgradeActionArg);
    } catch (Exception e) {
      logAndThrow(e, "SCM upgrade action for version " + version + " failed.",
          UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED);
    }
  }

  @Override
  protected ComponentVersion computeApparentVersion(int serializedVersion) throws IOException {
    return HDDSVersionUtils.deserializedPersistedApparentVersion(serializedVersion);
  }
}

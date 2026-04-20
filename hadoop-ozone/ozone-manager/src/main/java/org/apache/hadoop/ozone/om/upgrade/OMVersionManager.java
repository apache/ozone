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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component version manager for Ozone Manager.
 */
public class OMVersionManager extends ComponentVersionManager {

  private static final Logger LOG = LoggerFactory.getLogger(OMVersionManager.class);

  private final Map<ComponentVersion, OmUpgradeAction> upgradeActions;

  // The OM may not be fully initialized when the version manager is constructed. This field is just provided as an
  // argument for upgrade actions when they are run.
  private final OzoneManager upgradeActionArg;

  public OMVersionManager(OMStorage storage, OzoneManager upgradeActionArg) throws IOException {
    this(storage, upgradeActionArg, new OMUpgradeActionProvider());
  }

  public OMVersionManager(OMStorage storage, OzoneManager upgradeActionArg,
      ComponentUpgradeActionProvider<OmUpgradeAction> upgradeActionProvider) throws IOException {
    super(storage, computeApparentVersion(storage.getApparentVersion()), OzoneManagerVersion.SOFTWARE_VERSION);
    this.upgradeActionArg = upgradeActionArg;
    upgradeActions = upgradeActionProvider.load();
  }

  public void validateDBVersion(OMMetadataManager metadataManager) throws IOException {
    ComponentVersion dbVersion = getApparentVersionInDB(metadataManager);
    ComponentVersion apparentVersion = getApparentVersion();

    if (!apparentVersion.equals(dbVersion)) {
      LOG.info("Version file has different apparent version ({}) than OM DB ({}). That is expected if this "
          + "OM has never been finalized to a newer version.", apparentVersion, dbVersion);
    }
  }

  public void finalizeFromSnapshotIfRequired(OMMetadataManager metadataManager) throws IOException {
    ComponentVersion apparentVersionInDB = getApparentVersionInDB(metadataManager);
    if (apparentVersionInDB != null && !isAllowed(apparentVersionInDB)) {
      LOG.info("New OM snapshot received with higher apparent version {}. "
          + "Attempting to finalize current OM to that version.", apparentVersionInDB);
      finalizeUpgrade();
      updateApparentVersionInDB(metadataManager);
    }
  }

  @VisibleForTesting
  public Map<ComponentVersion, OmUpgradeAction> getUpgradeActionsForTesting() {
    return upgradeActions;
  }

  @Override
  protected void runUpgradeAction(ComponentVersion componentVersion) throws UpgradeException {
    OmUpgradeAction action = upgradeActions.get(componentVersion);
    if (action == null) {
      return;
    }
    try {
      action.execute(upgradeActionArg);
    } catch (Exception e) {
      logAndThrow(e, "OM upgrade action for version " + componentVersion + " failed.",
          UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED);
    }
  }

  private static ComponentVersion getApparentVersionInDB(OMMetadataManager metadataManager) throws IOException {
    String apparentVersion = metadataManager.getMetaTable().get(LAYOUT_VERSION_KEY);
    return (apparentVersion == null) ? null : computeApparentVersion(Integer.parseInt(apparentVersion));
  }

  private void updateApparentVersionInDB(OMMetadataManager metadataManager) throws IOException {
    metadataManager.getMetaTable().put(LAYOUT_VERSION_KEY, String.valueOf(getApparentVersion().serialize()));
  }

  /**
   * Maps a serialized apparent version to a {@link ComponentVersion}.
   * If the value is &gt;= {@link OzoneManagerVersion#ZDU} serialized, the OM has been finalized for ZDU and the
   * apparent version is resolved via {@link OzoneManagerVersion#deserialize(int)}. Values with no matching
   * {@link OzoneManagerVersion} fail startup with the persisted integer in the exception message.
   * If the value is below that threshold, the apparent version is resolved as an {@link OMLayoutFeature}. Integers in
   * the gap between the largest {@link OMLayoutFeature} and ZDU are not valid legacy layout values; startup fails with
   * the persisted integer in the exception message.
   */
  private static ComponentVersion computeApparentVersion(int serializedApparentVersion) throws IOException {
    if (serializedApparentVersion >= OzoneManagerVersion.ZDU.serialize()) {
      OzoneManagerVersion fromOm = OzoneManagerVersion.deserialize(serializedApparentVersion);
      if (fromOm != OzoneManagerVersion.FUTURE_VERSION) {
        return fromOm;
      }
    } else {
      ComponentVersion fromLayout = OMLayoutFeature.deserialize(serializedApparentVersion);
      if (fromLayout != null) {
        return fromLayout;
      }
    }
    throw new IOException("Initialization failed. Disk contains unknown apparent version " + serializedApparentVersion +
        " for software version " + OzoneManagerVersion.SOFTWARE_VERSION + ". Make sure OM was not downgraded after" +
        " finalization");
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.container.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.SCM_HA;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.DATANODE;

import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Action to run upgrade flow for SCM HA exactly once.
 */
@UpgradeActionHdds(feature = SCM_HA, component = DATANODE,
    type = ON_FINALIZE)
public class ScmHAFinalizeUpgradeActionDatanode
    implements HDDSUpgradeAction<DatanodeStateMachine> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ScmHAFinalizeUpgradeActionDatanode.class);

  @Override
  public void execute(DatanodeStateMachine dsm) throws Exception {
    LOG.info("Upgrading Datanode container layout for SCM HA support.");
    String clusterID = dsm.getLayoutStorage().getClusterID();

    for (StorageVolume volume:
        dsm.getContainer().getVolumeSet().getVolumesList()) {
      upgradeVolume(volume, clusterID);
    }
  }

  public static void upgradeVolume(StorageVolume volume, String clusterID)
      throws IOException  {
    File hddsVolumeDir = volume.getStorageDir();
    File clusterIDDir = new File(hddsVolumeDir, clusterID);
    File[] storageDirs = volume.getStorageDir().listFiles(File::isDirectory);

    if (storageDirs == null) {
      LOG.error("IO error for the volume {}. " +
          "Unable to process it for finalizing layout for SCM HA" +
          "support. Formatting will be retried on datanode restart.",
          volume.getStorageDir());
    }  else if (storageDirs.length == 0) {
      LOG.info("Skipping finalize for SCM HA for unformatted volume {}, no " +
          "action required.", hddsVolumeDir);
    } else if (storageDirs.length == 1) {
      if (!clusterIDDir.exists()) {
        // If the one directory is not the cluster ID directory, assume it is
        // the old SCM ID directory.
        File scmIDDir = storageDirs[0];
        LOG.info("Creating symlink {} -> {} as part of SCM HA " +
            "finalization for datanode.", clusterIDDir.getAbsolutePath(),
            scmIDDir.getAbsolutePath());
        Path relativeScmIDDir =
            hddsVolumeDir.toPath().relativize(scmIDDir.toPath());
        Files.createSymbolicLink(clusterIDDir.toPath(), relativeScmIDDir);
      } else {
        LOG.info("Volume already contains cluster ID directory {}. No " +
            "action required for SCM HA finalization.", clusterIDDir);
      }
    } else {
      // More than one subdirectory. As long as the cluster ID directory
      // exists we are ok.
      if (!clusterIDDir.exists()) {
        LOG.error("Volume {} is in Inconsistent state. Expected directory" +
            "{} not found.", hddsVolumeDir, clusterIDDir);
      } else {
        LOG.info("Volume already contains cluster ID directory {}. No " +
            "action required for SCM HA finalization.", clusterIDDir);
      }
    }
  }
}

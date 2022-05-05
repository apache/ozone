/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.upgrade;

import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.DATANODE_SCHEMA_V3;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.DATANODE;

/**
 * Upgrade Action for DataNode for SCHEMA V3.
 */
@UpgradeActionHdds(feature = DATANODE_SCHEMA_V3, component = DATANODE,
    type = ON_FINALIZE)
public class DatanodeSchemaV3FinalizeAction
    implements HDDSUpgradeAction<DatanodeStateMachine> {

  public static final Logger LOG =
      LoggerFactory.getLogger(DatanodeSchemaV3FinalizeAction.class);

  @Override
  public void execute(DatanodeStateMachine dsm) throws Exception {
    // Create RocksDB for each HddsVolume, no matter
    // hdds.datanode.container.schema.v3.enabled is true or false.
    LOG.info("Upgrading Datanode volume layout for Schema V3 support.");
    dbVolumeSet = HddsServerUtil.getDatanodeDbDirs(dsm.getco).isEmpty() ? null :
        new MutableVolumeSet(datanodeDetails.getUuidString(), conf,
            context, StorageVolume.VolumeType.DB_VOLUME, volumeChecker);
    HddsVolumeUtil.loadAllHddsVolumeDbStore(volumeSet, dbVolumeSet, LOG);

    MutableVolumeSet hddsVolumeSet = dsm.getContainer().getVolumeSet();
    MutableVolumeSet dbVolumeSet = dsm.getContainer().getDbVolumeSet();

    Preconditions.assertNotNull(hddsVolumeSet, "Data Volume should be null");
    hddsVolumeSet.writeLock();
    try {
      for (StorageVolume hddsVolume : hddsVolumeSet.getVolumesList()) {
        HddsVolume dataVolume = (HddsVolume) hddsVolume;
        if (dataVolume.getDbParentDir() != null) {
          // The RocksDB for this hddsVolume is already created(In upgrade
          // retry case).
          continue;
        }
        dataVolume.createDbStore(dbVolumeSet);
      }
    } finally {
      hddsVolumeSet.writeUnlock();
    }
  }
}


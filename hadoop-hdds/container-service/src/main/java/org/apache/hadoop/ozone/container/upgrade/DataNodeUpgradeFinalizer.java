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

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_IN_PROGRESS;

import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

/**
 * UpgradeFinalizer for the DataNode.
 */
public class DataNodeUpgradeFinalizer extends
    BasicUpgradeFinalizer<DatanodeStateMachine, HDDSLayoutVersionManager> {

  public DataNodeUpgradeFinalizer(HDDSLayoutVersionManager versionManager) {
    super(versionManager);
  }

  @Override
  public void preFinalizeUpgrade(DatanodeStateMachine dsm) {
    getVersionManager().setUpgradeState(FINALIZATION_IN_PROGRESS);
  }

  @Override
  public void finalizeLayoutFeature(LayoutFeature layoutFeature,
      DatanodeStateMachine dsm) throws UpgradeException {
    if (layoutFeature instanceof HDDSLayoutFeature) {
      HDDSLayoutFeature hddslayoutFeature =  (HDDSLayoutFeature)layoutFeature;
      super.finalizeLayoutFeature(hddslayoutFeature,
          hddslayoutFeature
              .datanodeAction(),
          dsm.getLayoutStorage());
    } else {
      String msg = String.format("Failed to finalize datanode layout feature " +
          "%s. It is not an HDDS Layout Feature.", layoutFeature);
      throw new UpgradeException(msg,
          UpgradeException.ResultCodes.LAYOUT_FEATURE_FINALIZATION_FAILED);
    }
  }
}

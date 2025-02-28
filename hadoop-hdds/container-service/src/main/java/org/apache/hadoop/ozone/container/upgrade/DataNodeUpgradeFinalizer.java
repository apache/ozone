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

import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.PREFINALIZE_VALIDATION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_IN_PROGRESS;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_REQUIRED;

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
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
  public void preFinalizeUpgrade(DatanodeStateMachine dsm)
      throws IOException {
    if (!canFinalizeDataNode(dsm)) {
      // DataNode is not yet ready to finalize.
      // Reset the Finalization state.
      getVersionManager().setUpgradeState(FINALIZATION_REQUIRED);
      String msg = "Pre Finalization checks failed on the DataNode.";
      logAndEmit(msg);
      throw new UpgradeException(msg, PREFINALIZE_VALIDATION_FAILED);
    }
    getVersionManager().setUpgradeState(FINALIZATION_IN_PROGRESS);
  }

  private boolean canFinalizeDataNode(DatanodeStateMachine dsm) {
    // Lets be sure that we do not have any open container before we return
    // from here. This function should be called in its own finalizer thread
    // context.
    for (Container<?> ctr :
        dsm.getContainer().getController().getContainers()) {
      ContainerProtos.ContainerDataProto.State state = ctr.getContainerState();
      long id = ctr.getContainerData().getContainerID();
      switch (state) {
      case OPEN:
      case CLOSING:
        LOG.warn("FinalizeUpgrade : Waiting for container {} to close, current "
            + "state is: {}", id, state);
        return false;
      default:
      }
    }
    return true;
  }

  @Override
  public void finalizeLayoutFeature(LayoutFeature layoutFeature,
      DatanodeStateMachine dsm) throws UpgradeException {
    if (layoutFeature instanceof HDDSLayoutFeature) {
      HDDSLayoutFeature hddslayoutFeature =  (HDDSLayoutFeature)layoutFeature;
      super.finalizeLayoutFeature(hddslayoutFeature,
          hddslayoutFeature
              .datanodeAction(LayoutFeature.UpgradeActionType.ON_FINALIZE),
          dsm.getLayoutStorage());
    } else {
      String msg = String.format("Failed to finalize datanode layout feature " +
          "%s. It is not an HDDS Layout Feature.", layoutFeature);
      throw new UpgradeException(msg,
          UpgradeException.ResultCodes.LAYOUT_FEATURE_FINALIZATION_FAILED);
    }
  }

  @Override
  public void runPrefinalizeStateActions(Storage storage,
                                         DatanodeStateMachine dsm)
      throws IOException {
    super.runPrefinalizeStateActions(
        lf -> ((HDDSLayoutFeature) lf)::datanodeAction, storage, dsm);
  }
}

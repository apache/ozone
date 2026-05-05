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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;
import static org.apache.hadoop.ozone.OzoneConsts.FORCE_LEASE_RECOVERY_ENV;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles lease recovery call between client and DN.
 */
public final class LeaseRecoveryClientDNHandler {
  static final Logger LOG = LoggerFactory.getLogger(LeaseRecoveryClientDNHandler.class);

  private LeaseRecoveryClientDNHandler() {
      // Not required.
  }

  /**
   * Get actual block length from DN for the last/penultimate block and update in KeyLocationInfo.
   * @param leaseKeyInfo keyInfo received from OM
   * @param adapter client adapter
   * @param forceRecovery whether to do force recovery
   * @return List<OmKeyLocationInfo>
   */
  @Nonnull
  public static List<OmKeyLocationInfo> getOmKeyLocationInfos(LeaseKeyInfo leaseKeyInfo,
      OzoneClientAdapter adapter, boolean forceRecovery) throws IOException {
    OmKeyLocationInfoGroup keyLatestVersionLocations = leaseKeyInfo.getKeyInfo().getLatestVersionLocations();
    List<OmKeyLocationInfo> keyLocationInfoList = keyLatestVersionLocations.getLocationList();
    OmKeyLocationInfoGroup openKeyLatestVersionLocations = leaseKeyInfo.getOpenKeyInfo().getLatestVersionLocations();
    List<OmKeyLocationInfo> openKeyLocationInfoList = openKeyLatestVersionLocations.getLocationList();

    int openKeyLocationSize = openKeyLocationInfoList.size();
    int keyLocationSize = keyLocationInfoList.size();
    OmKeyLocationInfo openKeyFinalBlock = null;
    OmKeyLocationInfo openKeyPenultimateBlock = null;
    OmKeyLocationInfo keyFinalBlock;

    if (keyLocationSize > 0) {
      // Block info from fileTable
      keyFinalBlock = keyLocationInfoList.get(keyLocationSize - 1);
      // Block info from openFileTable
      if (openKeyLocationSize > 1) {
        openKeyFinalBlock = openKeyLocationInfoList.get(openKeyLocationSize - 1);
        openKeyPenultimateBlock = openKeyLocationInfoList.get(openKeyLocationSize - 2);
      } else if (openKeyLocationSize > 0) {
        openKeyFinalBlock = openKeyLocationInfoList.get(0);
      }
      // Finalize the final block and get block length
      try {
        // CASE 1: When openFileTable has more block than fileTable
        // Try to finalize last block of openFileTable
        // Add that block into fileTable locationInfo
        if (openKeyLocationSize > keyLocationSize) {
          openKeyFinalBlock.setLength(adapter.finalizeBlock(openKeyFinalBlock));
          keyLocationInfoList.add(openKeyFinalBlock);
        }
        // CASE 2: When openFileTable penultimate block length is not equal to fileTable block length of last block
        // Finalize and get the actual block length and update in fileTable last block
        if ((openKeyPenultimateBlock != null && keyFinalBlock != null) &&
            openKeyPenultimateBlock.getLength() != keyFinalBlock.getLength() &&
            openKeyPenultimateBlock.getBlockID().getLocalID() == keyFinalBlock.getBlockID().getLocalID()) {
          keyFinalBlock.setLength(adapter.finalizeBlock(keyFinalBlock));
        }
        // CASE 3: When openFileTable has same number of blocks as fileTable
        // Finalize and get actual length of fileTable final block
        if (keyLocationInfoList.size() == openKeyLocationInfoList.size() && keyFinalBlock != null) {
          keyFinalBlock.setLength(adapter.finalizeBlock(keyFinalBlock));
        }
      } catch (Throwable e) {
        if (e instanceof StorageContainerException &&
            (((StorageContainerException) e).getResult().equals(NO_SUCH_BLOCK)
            || ((StorageContainerException) e).getResult().equals(CONTAINER_NOT_FOUND))
            && openKeyPenultimateBlock != null && keyFinalBlock != null &&
            openKeyPenultimateBlock.getBlockID().getLocalID() == keyFinalBlock.getBlockID().getLocalID()) {
          try {
            keyFinalBlock.setLength(adapter.finalizeBlock(keyFinalBlock));
          } catch (Throwable exp) {
            if (!forceRecovery) {
              throw exp;
            }
            LOG.warn("Failed to finalize block. Continue to recover the file since {} is enabled.",
                FORCE_LEASE_RECOVERY_ENV, exp);
          }
        } else if (!forceRecovery) {
          throw e;
        } else {
          LOG.warn("Failed to finalize block. Continue to recover the file since {} is enabled.",
              FORCE_LEASE_RECOVERY_ENV, e);
        }
      }
    }
    return keyLocationInfoList;
  }
}

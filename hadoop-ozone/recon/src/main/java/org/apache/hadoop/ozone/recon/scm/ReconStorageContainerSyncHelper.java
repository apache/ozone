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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE_DEFAULT;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReconStorageContainerSyncHelper {

  // Serialized size of one ContainerID proto on the wire (varint tag + 8-byte long = ~12 bytes).
  // Used to derive the maximum batch size that fits within ipc.maximum.data.length.
  private static final long CONTAINER_ID_PROTO_SIZE_BYTES = 12;

  private static final Logger LOG = LoggerFactory
      .getLogger(ReconStorageContainerSyncHelper.class);

  private final StorageContainerServiceProvider scmServiceProvider;
  private final OzoneConfiguration ozoneConfiguration;
  private final ReconContainerManager containerManager;

  ReconStorageContainerSyncHelper(StorageContainerServiceProvider scmServiceProvider,
                                  OzoneConfiguration ozoneConfiguration,
                                  ReconContainerManager containerManager) {
    this.scmServiceProvider = scmServiceProvider;
    this.ozoneConfiguration = ozoneConfiguration;
    this.containerManager = containerManager;
  }

  public boolean syncWithSCMContainerInfo() {
    try {
      long totalContainerCount = scmServiceProvider.getContainerCount(
          HddsProtos.LifeCycleState.CLOSED);
      long containerCountPerCall =
          getContainerCountPerCall(totalContainerCount);
      ContainerID startContainerId = ContainerID.valueOf(1);
      long retrievedContainerCount = 0;
      if (totalContainerCount > 0) {
        while (retrievedContainerCount < totalContainerCount) {
          List<ContainerID> listOfContainers = scmServiceProvider.
              getListOfContainerIDs(startContainerId,
                  Long.valueOf(containerCountPerCall).intValue(),
                  HddsProtos.LifeCycleState.CLOSED);
          if (null != listOfContainers && !listOfContainers.isEmpty()) {
            LOG.info("Got list of containers from SCM : {}", listOfContainers.size());
            listOfContainers.forEach(containerID -> {
              boolean isContainerPresentAtRecon = containerManager.containerExist(containerID);
              if (!isContainerPresentAtRecon) {
                try {
                  ContainerWithPipeline containerWithPipeline =
                      scmServiceProvider.getContainerWithPipeline(
                          containerID.getId());
                  containerManager.addNewContainer(containerWithPipeline);
                } catch (IOException e) {
                  LOG.error("Could not get container with pipeline " +
                      "for container : {}", containerID);
                }
              }
            });
            long lastID = listOfContainers.get(listOfContainers.size() - 1).getId();
            startContainerId = ContainerID.valueOf(lastID + 1);
          } else {
            LOG.info("No containers found at SCM in CLOSED state");
            return false;
          }
          retrievedContainerCount += containerCountPerCall;
        }
      }
    } catch (Exception e) {
      LOG.error("Unable to refresh Recon SCM DB Snapshot. ", e);
      return false;
    }
    return true;
  }

  private long getContainerCountPerCall(long totalContainerCount) {
    long hadoopRPCSize = ozoneConfiguration.getInt(
        IPC_MAXIMUM_DATA_LENGTH, IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    long countByRpcLimit = hadoopRPCSize / CONTAINER_ID_PROTO_SIZE_BYTES;
    long countByBatchLimit = ozoneConfiguration.getLong(
        OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE,
        OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE_DEFAULT);

    long batchSize = Math.min(countByRpcLimit, countByBatchLimit);
    return Math.min(totalContainerCount, batchSize);
  }
}

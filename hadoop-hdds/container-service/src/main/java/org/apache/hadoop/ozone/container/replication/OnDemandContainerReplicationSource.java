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

package org.apache.hadoop.ozone.container.replication;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.REPLICATION_LIMIT_REACHED;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A naive implementation of the replication source which creates a tar file
 * on-demand without pre-create the compressed archives.
 */
public class OnDemandContainerReplicationSource
    implements ContainerReplicationSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(OnDemandContainerReplicationSource.class);

  private final ContainerController controller;
  private final ReplicationServer.ReplicationConfig config;

  public OnDemandContainerReplicationSource(
      ContainerController controller,
      ReplicationServer.ReplicationConfig config) {
    this.controller = controller;
    this.config = config;
  }

  @Override
  public void prepare(long containerId) {
    // no pre-create in this implementation
  }

  @Override
  public void copyData(long containerId, OutputStream destination,
                       CopyContainerCompression compression)
      throws IOException {

    Container container = controller.getContainer(containerId);

    if (container == null) {
      throw new StorageContainerException("Container " + containerId +
          " is not found.", CONTAINER_NOT_FOUND);
    }

    HddsVolume volume = (HddsVolume) container.getContainerData().getVolume();
    if (volume != null) {
      if (volume.getActiveOutboundReplications() >=
          config.getVolumeOutboundLimit()) {
        LOG.info("Volume {} has reached the maximum number of concurrent " +
                "replication reads ({})", volume.getStorageID(),
            config.getVolumeOutboundLimit());
        throw new StorageContainerException("Volume " + volume.getStorageID() +
            " has reached the maximum number of concurrent replication reads ("
            + config.getVolumeOutboundLimit() + ")", REPLICATION_LIMIT_REACHED);
      }
      volume.incActiveOutboundReplications();
    }
    try {
      controller.exportContainer(
          container.getContainerType(), containerId, destination,
          new TarContainerPacker(compression));
    } finally {
      if (volume != null) {
        volume.decActiveOutboundReplications();
      }
    }
  }
}

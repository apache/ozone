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

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;

/**
 * A naive implementation of the replication source which creates a tar file
 * on-demand without pre-create the compressed archives.
 */
public class OnDemandContainerReplicationSource
    implements ContainerReplicationSource {

  private final ContainerController controller;

  public OnDemandContainerReplicationSource(
      ContainerController controller) {
    this.controller = controller;
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

    controller.exportContainer(
        container.getContainerType(), containerId, destination,
        new TarContainerPacker(compression));
  }
}

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

package org.apache.hadoop.ozone.container.ozoneimpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Control plane for container management in datanode.
 */
public class ContainerController {

  private final ContainerSet containerSet;
  private final Map<ContainerType, Handler> handlers;
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerController.class);

  public ContainerController(final ContainerSet containerSet,
      final Map<ContainerType, Handler> handlers) {
    this.containerSet = containerSet;
    this.handlers = handlers;
  }

  /**
   * Returns the Container given a container id.
   *
   * @param containerId ID of the container
   * @return Container
   */
  public Container getContainer(final long containerId) {
    return containerSet.getContainer(containerId);
  }

  public String getContainerLocation(final long containerId) {
    Container cont = containerSet.getContainer(containerId);
    if (cont != null) {
      return cont.getContainerData().getContainerPath();
    } else {
      return "nonexistent";
    }
  }

  /**
   * Marks the container for closing. Moves the container to CLOSING state.
   *
   * @param containerId Id of the container to update
   * @throws IOException in case of exception
   */
  public void markContainerForClose(final long containerId)
      throws IOException {
    Container container = containerSet.getContainer(containerId);
    if (container == null) {
      String warning;
      Set<Long> missingContainerSet = containerSet.getMissingContainerSet();
      if (missingContainerSet.contains(containerId)) {
        warning = "The Container is in the MissingContainerSet " +
                "hence we can't close it. ContainerID: " + containerId;
      } else {
        warning = "The Container is not found. ContainerID: " + containerId;
      }
      LOG.warn(warning);
      throw new ContainerNotFoundException(ContainerID.valueOf(containerId));
    } else {
      if (container.getContainerState() == State.OPEN) {
        getHandler(container).markContainerForClose(container);
      }
    }
  }

  /**
   * Marks the container as UNHEALTHY.
   *
   * @param containerId Id of the container to update
   * @param reason The reason the container was marked unhealthy
   * @throws IOException in case of exception
   */
  public boolean markContainerUnhealthy(final long containerId, ScanResult reason)
          throws IOException {
    Container container = getContainer(containerId);
    if (container == null) {
      LOG.warn("Container {} not found, may be deleted, skip marking UNHEALTHY", containerId);
      return false;
    } else if (container.getContainerState() == State.UNHEALTHY) {
      LOG.debug("Container {} is already UNHEALTHY, skip marking UNHEALTHY", containerId);
      return false;
    } else {
      getHandler(container).markContainerUnhealthy(container, reason);
      return true;
    }
  }

  /**
   * Updates the container checksum information on disk and in memory.
   *
   * @param containerId The ID of the container to update
   * @param treeWriter The container merkle tree with the updated information about the container
   * @throws IOException For errors sending an ICR or updating the container checksum on disk. If the disk update
   * fails, the checksum in memory will not be updated.
   */
  public void updateContainerChecksum(long containerId, ContainerMerkleTreeWriter treeWriter)
      throws IOException {
    Container container = getContainer(containerId);
    if (container == null) {
      LOG.warn("Container {} not found, may be deleted, skip updating checksums", containerId);
    } else {
      getHandler(container).updateContainerChecksum(container, treeWriter);
    }
  }

  /**
   * Returns the container report.
   *
   * @return ContainerReportsProto
   * @throws IOException in case of exception
   */
  public ContainerReportsProto getContainerReport()
      throws IOException {
    return containerSet.getContainerReport();
  }

  /**
   * Quasi closes a container given its id.
   *
   * @param containerId Id of the container to quasi close
   * @param reason The reason the container was quasi closed, for logging
   *               purposes.
   * @throws IOException in case of exception
   */
  public void quasiCloseContainer(final long containerId, String reason)
      throws IOException {
    final Container container = containerSet.getContainer(containerId);
    getHandler(container).quasiCloseContainer(container, reason);
  }

  /**
   * Closes a container given its Id.
   *
   * @param containerId Id of the container to close
   * @throws IOException in case of exception
   */
  public void closeContainer(final long containerId) throws IOException {
    final Container container = containerSet.getContainer(containerId);
    getHandler(container).closeContainer(container);
  }

  /**
   * Returns the Container given a container id.
   *
   * @param containerId ID of the container
   */
  public void addFinalizedBlock(final long containerId,
      final long localId) {
    Container container = containerSet.getContainer(containerId);
    if (container != null) {
      getHandler(container).addFinalizedBlock(container, localId);
    }
  }

  public boolean isFinalizedBlockExist(final long containerId,
      final long localId) {
    Container container = containerSet.getContainer(containerId);
    if (container != null) {
      return getHandler(container).isFinalizedBlockExist(container, localId);
    }
    return false;
  }

  public Container importContainer(
      final ContainerData containerData,
      final InputStream rawContainerStream,
      final TarContainerPacker packer) throws IOException {
    return handlers.get(containerData.getContainerType())
        .importContainer(containerData, rawContainerStream, packer);
  }

  public void exportContainer(final ContainerType type,
      final long containerId, final OutputStream outputStream,
      final TarContainerPacker packer) throws IOException {
    try {
      handlers.get(type).exportContainer(
          containerSet.getContainer(containerId), outputStream, packer);
    } catch (IOException e) {
      // If export fails, then trigger a scan for the container
      containerSet.scanContainer(containerId, "Export failed");
      throw e;
    }
  }

  /**
   * Deletes a container given its Id.
   * @param containerId Id of the container to be deleted
   * @param force if this is set to true, we delete container without checking
   * state of the container.
   */
  public void deleteContainer(final long containerId, boolean force)
      throws IOException {
    final Container container = containerSet.getContainer(containerId);
    if (container != null) {
      getHandler(container).deleteContainer(container, force);
    }
  }

  public void reconcileContainer(DNContainerOperationClient dnClient, long containerID, Set<DatanodeDetails> peers)
      throws IOException {
    Container<?> container = containerSet.getContainer(containerID);
    if (container == null) {
      LOG.warn("Container {} to reconcile not found on this datanode.", containerID);
    } else {
      getHandler(container).reconcileContainer(dnClient, container, peers);
    }
  }

  /**
   * Given a container, returns its handler instance.
   *
   * @param container Container
   * @return handler of the container
   */
  private Handler getHandler(final Container container) {
    return handlers.get(container.getContainerType());
  }

  public Iterable<Container<?>> getContainers() {
    return containerSet;
  }

  /**
   * Return an iterator of containers which are associated with the specified
   * <code>volume</code>.
   *
   * @param  volume the HDDS volume which should be used to filter containers
   * @return {@literal Iterator<Container>}
   */
  public Iterator<Container<?>> getContainers(HddsVolume volume) {
    return containerSet.getContainerIterator(volume);
  }

  /**
   * Get the number of containers based on the given volume.
   *
   * @param volume hdds volume.
   * @return number of containers.
   */
  public long getContainerCount(HddsVolume volume) {
    return containerSet.containerCount(volume);
  }

  void updateDataScanTimestamp(long containerId, Instant timestamp)
      throws IOException {
    Container container = containerSet.getContainer(containerId);
    if (container != null) {
      container.updateDataScanTimestamp(timestamp);
    } else {
      LOG.warn("Container {} not found, may be deleted, " +
          "skip update DataScanTimestamp", containerId);
    }
  }

}

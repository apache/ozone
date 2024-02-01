/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.interfaces;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.ratis.statemachine.StateMachine;

import static org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;

/**
 * Dispatcher sends ContainerCommandRequests to Handler. Each Container Type
 * should have an implementation for Handler.
 */
@SuppressWarnings("visibilitymodifier")
public abstract class Handler {

  protected final ConfigurationSource conf;
  protected final ContainerSet containerSet;
  protected final VolumeSet volumeSet;
  protected String clusterId;
  protected final ContainerMetrics metrics;
  protected String datanodeId;
  private IncrementalReportSender<Container> icrSender;

  protected Handler(ConfigurationSource config, String datanodeId,
      ContainerSet contSet, VolumeSet volumeSet,
      ContainerMetrics containerMetrics,
      IncrementalReportSender<Container> icrSender) {
    this.conf = config;
    this.containerSet = contSet;
    this.volumeSet = volumeSet;
    this.metrics = containerMetrics;
    this.datanodeId = datanodeId;
    this.icrSender = icrSender;
  }

  public static Handler getHandlerForContainerType(
      final ContainerType containerType, final ConfigurationSource config,
      final String datanodeId, final ContainerSet contSet,
      final VolumeSet volumeSet, final ContainerMetrics metrics,
      IncrementalReportSender<Container> icrSender) {
    switch (containerType) {
    case KeyValueContainer:
      return new KeyValueHandler(config,
          datanodeId, contSet, volumeSet, metrics,
          icrSender);
    default:
      throw new IllegalArgumentException("Handler for ContainerType: " +
          containerType + "doesn't exist.");
    }
  }

  public abstract StateMachine.DataChannel getStreamDataChannel(
          Container container, ContainerCommandRequestProto msg)
          throws StorageContainerException;

  /**
   * Returns the Id of this datanode.
   *
   * @return datanode Id
   */
  protected String getDatanodeId() {
    return datanodeId;
  }

  /**
   * This should be called whenever there is state change. It will trigger
   * an ICR to SCM.
   *
   * @param container Container for which ICR has to be sent
   */
  protected void sendICR(final Container container)
      throws StorageContainerException {
    if (container
        .getContainerState() == ContainerProtos.ContainerDataProto
        .State.RECOVERING) {
      // Ignoring the recovering containers reports for now.
      return;
    }
    icrSender.send(container);
  }

  public abstract ContainerCommandResponseProto handle(
      ContainerCommandRequestProto msg, Container container,
      DispatcherContext dispatcherContext);

  /**
   * Imports container from a raw input stream.
   */
  public abstract Container importContainer(
      ContainerData containerData, InputStream rawContainerStream,
      TarContainerPacker packer)
      throws IOException;

  /**
   * Exports container to the output stream.
   */
  public abstract void exportContainer(
      Container container,
      OutputStream outputStream,
      TarContainerPacker packer)
      throws IOException;

  /**
   * Stop the Handler.
   */
  public abstract void stop();

  /**
   * Marks the container for closing. Moves the container to CLOSING state.
   *
   * @param container container to update
   * @throws IOException in case of exception
   */
  public abstract void markContainerForClose(Container container)
      throws IOException;

  /**
   * Marks the container Unhealthy. Moves the container to UNHEALTHY state.
   *
   * @param container container to update
   * @param reason The reason the container was marked unhealthy
   * @throws IOException in case of exception
   */
  public abstract void markContainerUnhealthy(Container container,
                                              ScanResult reason)
      throws IOException;

  /**
   * Moves the Container to QUASI_CLOSED state.
   *
   * @param container container to be quasi closed
   * @param reason The reason the container was quasi closed, for logging
   *               purposes.
   * @throws IOException
   */
  public abstract void quasiCloseContainer(Container container, String reason)
      throws IOException;

  /**
   * Moves the Container to CLOSED state.
   *
   * @param container container to be closed
   * @throws IOException
   */
  public abstract void closeContainer(Container container)
      throws IOException;

  /**
   * Deletes the given container.
   *
   * @param container container to be deleted
   * @param force     if this is set to true, we delete container without
   *                  checking
   *                  state of the container.
   * @throws IOException
   */
  public abstract void deleteContainer(Container container, boolean force)
      throws IOException;

  /**
   * Deletes the given files associated with a block of the container.
   *
   * @param container container whose block is to be deleted
   * @param blockData block to be deleted
   * @throws IOException
   */
  public abstract void deleteBlock(Container container, BlockData blockData)
      throws IOException;

  /**
   * Deletes the possible onDisk but unreferenced blocks/chunks with localID
   * in the container.
   *
   * @param container container whose block/chunk is to be deleted
   * @param localID   localId of the block/chunk
   * @throws IOException
   */
  public abstract void deleteUnreferenced(Container container, long localID)
      throws IOException;

  public void setClusterID(String clusterID) {
    this.clusterId = clusterID;
  }

  /**
   * Copy container to the destination path.
   */
  public abstract void copyContainer(
      Container container, Path destination)
      throws IOException;

  /**
   * Imports container from a container path.
   */
  public abstract Container importContainer(
      ContainerData containerData, Path containerPath) throws IOException;
}


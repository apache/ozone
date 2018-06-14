/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.common.impl.ContainerManagerImpl;
import org.apache.hadoop.ozone.container.common.impl.Dispatcher;
import org.apache.hadoop.ozone.container.common.impl.KeyManagerImpl;
import org.apache.hadoop.ozone.container.common.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.interfaces.KeyManager;
import org.apache.hadoop.ozone.container.common.statemachine.background
    .BlockDeletingService;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServer;
import org.apache.hadoop.ozone.container.common.transport.server
    .XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server
    .XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis
    .XceiverServerRatis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_ROOT_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.INVALID_PORT;

/**
 * Ozone main class sets up the network server and initializes the container
 * layer.
 */
public class OzoneContainer {
  public static final Logger LOG =
      LoggerFactory.getLogger(OzoneContainer.class);

  private final Configuration ozoneConfig;
  private final ContainerDispatcher dispatcher;
  private final ContainerManager manager;
  private final XceiverServerSpi[] server;
  private final ChunkManager chunkManager;
  private final KeyManager keyManager;
  private final BlockDeletingService blockDeletingService;

  /**
   * Creates a network endpoint and enables Ozone container.
   *
   * @param ozoneConfig - Config
   * @throws IOException
   */
  public OzoneContainer(
      DatanodeDetails datanodeDetails, Configuration ozoneConfig)
      throws IOException {
    this.ozoneConfig = ozoneConfig;
    List<StorageLocation> locations = new LinkedList<>();
    String[] paths = ozoneConfig.getStrings(
        OzoneConfigKeys.OZONE_METADATA_DIRS);
    if (paths != null && paths.length > 0) {
      for (String p : paths) {
        locations.add(StorageLocation.parse(
            Paths.get(p).resolve(CONTAINER_ROOT_PREFIX).toString()));
      }
    } else {
      getDataDir(locations);
    }

    manager = new ContainerManagerImpl();
    manager.init(this.ozoneConfig, locations, datanodeDetails);
    this.chunkManager = new ChunkManagerImpl(manager);
    manager.setChunkManager(this.chunkManager);

    this.keyManager = new KeyManagerImpl(manager, ozoneConfig);
    manager.setKeyManager(this.keyManager);

    long svcInterval =
        ozoneConfig.getTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    long serviceTimeout = ozoneConfig.getTimeDuration(
        OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
        OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    this.blockDeletingService = new BlockDeletingService(manager,
        svcInterval, serviceTimeout, ozoneConfig);

    this.dispatcher = new Dispatcher(manager, this.ozoneConfig);

    boolean useGrpc = this.ozoneConfig.getBoolean(
        ScmConfigKeys.DFS_CONTAINER_GRPC_ENABLED_KEY,
        ScmConfigKeys.DFS_CONTAINER_GRPC_ENABLED_DEFAULT);
    server = new XceiverServerSpi[]{
        useGrpc ? new XceiverServerGrpc(datanodeDetails,
            this.ozoneConfig, this.dispatcher) :
            new XceiverServer(datanodeDetails,
                this.ozoneConfig, this.dispatcher),
      XceiverServerRatis
          .newXceiverServerRatis(datanodeDetails, this.ozoneConfig, dispatcher)
    };
  }

  /**
   * Starts serving requests to ozone container.
   *
   * @throws IOException
   */
  public void start() throws IOException {
    for (XceiverServerSpi serverinstance : server) {
      serverinstance.start();
    }
    blockDeletingService.start();
    dispatcher.init();
  }

  /**
   * Stops the ozone container.
   * <p>
   * Shutdown logic is not very obvious from the following code. if you need to
   * modify the logic, please keep these comments in mind. Here is the shutdown
   * sequence.
   * <p>
   * 1. We shutdown the network ports.
   * <p>
   * 2. Now we need to wait for all requests in-flight to finish.
   * <p>
   * 3. The container manager lock is a read-write lock with "Fairness"
   * enabled.
   * <p>
   * 4. This means that the waiting threads are served in a "first-come-first
   * -served" manner. Please note that this applies to waiting threads only.
   * <p>
   * 5. Since write locks are exclusive, if we are waiting to get a lock it
   * implies that we are waiting for in-flight operations to complete.
   * <p>
   * 6. if there are other write operations waiting on the reader-writer lock,
   * fairness guarantees that they will proceed before the shutdown lock
   * request.
   * <p>
   * 7. Since all operations either take a reader or writer lock of container
   * manager, we are guaranteed that we are the last operation since we have
   * closed the network port, and we wait until close is successful.
   * <p>
   * 8. We take the writer lock and call shutdown on each of the managers in
   * reverse order. That is chunkManager, keyManager and containerManager is
   * shutdown.
   */
  public void stop() {
    LOG.info("Attempting to stop container services.");
    for(XceiverServerSpi serverinstance: server) {
      serverinstance.stop();
    }
    dispatcher.shutdown();

    try {
      this.manager.writeLock();
      this.chunkManager.shutdown();
      this.keyManager.shutdown();
      this.manager.shutdown();
      this.blockDeletingService.shutdown();
      LOG.info("container services shutdown complete.");
    } catch (IOException ex) {
      LOG.warn("container service shutdown error:", ex);
    } finally {
      this.manager.writeUnlock();
    }
  }

  /**
   * Returns a paths to data dirs.
   *
   * @param pathList - List of paths.
   * @throws IOException
   */
  private void getDataDir(List<StorageLocation> pathList) throws IOException {
    for (String dir : ozoneConfig.getStrings(DFS_DATANODE_DATA_DIR_KEY)) {
      StorageLocation location = StorageLocation.parse(dir);
      pathList.add(location);
    }
  }

  /**
   * Returns node report of container storage usage.
   */
  public NodeReportProto getNodeReport() throws IOException {
    return this.manager.getNodeReport();
  }

  private int getPortbyType(HddsProtos.ReplicationType replicationType) {
    for (XceiverServerSpi serverinstance : server) {
      if (serverinstance.getServerType() == replicationType) {
        return serverinstance.getIPCPort();
      }
    }
    return INVALID_PORT;
  }

  /**
   * Returns the container server IPC port.
   *
   * @return Container server IPC port.
   */
  public int getContainerServerPort() {
    return getPortbyType(HddsProtos.ReplicationType.STAND_ALONE);
  }

  /**
   * Returns the Ratis container Server IPC port.
   *
   * @return Ratis port.
   */
  public int getRatisContainerServerPort() {
    return getPortbyType(HddsProtos.ReplicationType.RATIS);
  }

  /**
   * Returns container report.
   * @return - container report.
   * @throws IOException
   */
  public ContainerReportsProto getContainerReport() throws IOException {
    return this.manager.getContainerReport();
  }

// TODO: remove getContainerReports
  /**
   * Returns the list of closed containers.
   * @return - List of closed containers.
   * @throws IOException
   */
  public List<ContainerData> getClosedContainerReports() throws IOException {
    return this.manager.getClosedContainerReports();
  }

  private XceiverServerSpi getRatisSerer() {
    for (XceiverServerSpi serverInstance : server) {
      if (serverInstance instanceof XceiverServerRatis) {
        return serverInstance;
      }
    }
    return null;
  }

  private XceiverServerSpi getStandaAloneSerer() {
    for (XceiverServerSpi serverInstance : server) {
      if (!(serverInstance instanceof XceiverServerRatis)) {
        return serverInstance;
      }
    }
    return null;
  }

  @VisibleForTesting
  public ContainerManager getContainerManager() {
    return this.manager;
  }

  public void submitContainerRequest(
      ContainerProtos.ContainerCommandRequestProto request,
      HddsProtos.ReplicationType replicationType) throws IOException {
    XceiverServerSpi serverInstance;
    long containerId = getContainerIdForCmd(request);
    if (replicationType == HddsProtos.ReplicationType.RATIS) {
      serverInstance = getRatisSerer();
      Preconditions.checkNotNull(serverInstance);
      serverInstance.submitRequest(request);
      LOG.info("submitting {} request over RATIS server for container {}",
          request.getCmdType(), containerId);
    } else {
      serverInstance = getStandaAloneSerer();
      Preconditions.checkNotNull(serverInstance);
      getStandaAloneSerer().submitRequest(request);
      LOG.info(
          "submitting {} request over STAND_ALONE server for container {}",
          request.getCmdType(), containerId);
    }

  }

  private long getContainerIdForCmd(
      ContainerProtos.ContainerCommandRequestProto request)
      throws IllegalArgumentException {
    ContainerProtos.Type type = request.getCmdType();
    switch (type) {
    case CloseContainer:
      return request.getCloseContainer().getContainerID();
      // Right now, we handle only closeContainer via queuing it over the
      // over the XceiVerServer. For all other commands we throw Illegal
      // argument exception here. Will need to extend the switch cases
      // in case we want add another commands here.
    default:
      throw new IllegalArgumentException("Cmd " + request.getCmdType()
          + " not supported over HearBeat Response");
    }
  }
}
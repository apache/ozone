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

package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto
    .ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;

import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.BlockDeletingService;
import org.apache.hadoop.ozone.container.replication.GrpcReplicationService;
import org.apache.hadoop.ozone.container.replication
    .OnDemandContainerReplicationSource;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConfigKeys.*;

/**
 * Ozone main class sets up the network servers and initializes the container
 * layer.
 */
public class OzoneContainer {

  private static final Logger LOG = LoggerFactory.getLogger(
      OzoneContainer.class);

  private final HddsDispatcher hddsDispatcher;
  private final Map<ContainerType, Handler> handlers;
  private final OzoneConfiguration config;
  private final VolumeSet volumeSet;
  private final ContainerSet containerSet;
  private final XceiverServerSpi writeChannel;
  private final XceiverServerSpi readChannel;
  private final ContainerController controller;
  private ContainerMetadataScanner metadataScanner;
  private List<ContainerDataScanner> dataScanners;
  private final BlockDeletingService blockDeletingService;

  /**
   * Construct OzoneContainer object.
   * @param datanodeDetails
   * @param conf
   * @param certClient
   * @throws DiskOutOfSpaceException
   * @throws IOException
   */
  public OzoneContainer(DatanodeDetails datanodeDetails, OzoneConfiguration
      conf, StateContext context, CertificateClient certClient)
      throws IOException {
    this.config = conf;
    this.volumeSet = new VolumeSet(datanodeDetails.getUuidString(), conf);
    this.containerSet = new ContainerSet();
    this.metadataScanner = null;

    buildContainerSet();
    final ContainerMetrics metrics = ContainerMetrics.create(conf);
    this.handlers = Maps.newHashMap();
    for (ContainerType containerType : ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(
              containerType, conf, context, containerSet, volumeSet, metrics));
    }
    this.hddsDispatcher = new HddsDispatcher(config, containerSet, volumeSet,
        handlers, context, metrics);

    /*
     * ContainerController is the control plane
     * XceiverServerRatis is the write channel
     * XceiverServerGrpc is the read channel
     */
    this.controller = new ContainerController(containerSet, handlers);
    this.writeChannel = XceiverServerRatis.newXceiverServerRatis(
        datanodeDetails, config, hddsDispatcher, controller, certClient,
        context);
    this.readChannel = new XceiverServerGrpc(
        datanodeDetails, config, hddsDispatcher, certClient,
        createReplicationService());
    long svcInterval = config
        .getTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
            OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    long serviceTimeout = config
        .getTimeDuration(OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
            OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);
    this.blockDeletingService =
        new BlockDeletingService(this, svcInterval, serviceTimeout,
            TimeUnit.MILLISECONDS, config);
  }

  private GrpcReplicationService createReplicationService() {
    return new GrpcReplicationService(
        new OnDemandContainerReplicationSource(controller));
  }

  /**
   * Build's container map.
   */
  private void buildContainerSet() {
    Iterator<HddsVolume> volumeSetIterator = volumeSet.getVolumesList()
        .iterator();
    ArrayList<Thread> volumeThreads = new ArrayList<Thread>();

    //TODO: diskchecker should be run before this, to see how disks are.
    // And also handle disk failure tolerance need to be added
    while (volumeSetIterator.hasNext()) {
      HddsVolume volume = volumeSetIterator.next();
      Thread thread = new Thread(new ContainerReader(volumeSet, volume,
          containerSet, config));
      thread.start();
      volumeThreads.add(thread);
    }

    try {
      for (int i = 0; i < volumeThreads.size(); i++) {
        volumeThreads.get(i).join();
      }
    } catch (InterruptedException ex) {
      LOG.info("Volume Threads Interrupted exception", ex);
    }

  }


  /**
   * Start background daemon thread for performing container integrity checks.
   */
  private void startContainerScrub() {
    ContainerScrubberConfiguration c = config.getObject(
        ContainerScrubberConfiguration.class);
    boolean enabled = c.isEnabled();
    long metadataScanInterval = c.getMetadataScanInterval();
    long bytesPerSec = c.getBandwidthPerVolume();

    if (!enabled) {
      LOG.info("Background container scanner has been disabled.");
    } else {
      if (this.metadataScanner == null) {
        this.metadataScanner = new ContainerMetadataScanner(config, controller,
            metadataScanInterval);
      }
      this.metadataScanner.start();

      dataScanners = new ArrayList<>();
      for (HddsVolume v : volumeSet.getVolumesList()) {
        ContainerDataScanner s = new ContainerDataScanner(config, controller,
            v, bytesPerSec);
        s.start();
        dataScanners.add(s);
      }
    }
  }

  /**
   * Stop the scanner thread and wait for thread to die.
   */
  private void stopContainerScrub() {
    if (metadataScanner == null) {
      return;
    }
    metadataScanner.shutdown();
    metadataScanner = null;
    for (ContainerDataScanner s : dataScanners) {
      s.shutdown();
    }
  }

  /**
   * Starts serving requests to ozone container.
   *
   * @throws IOException
   */
  public void start(String scmId) throws IOException {
    LOG.info("Attempting to start container services.");
    startContainerScrub();
    writeChannel.start();
    readChannel.start();
    hddsDispatcher.init();
    hddsDispatcher.setScmId(scmId);
    blockDeletingService.start();
  }

  /**
   * Stop Container Service on the datanode.
   */
  public void stop() {
    //TODO: at end of container IO integration work.
    LOG.info("Attempting to stop container services.");
    stopContainerScrub();
    writeChannel.stop();
    readChannel.stop();
    this.handlers.values().forEach(Handler::stop);
    hddsDispatcher.shutdown();
    volumeSet.shutdown();
    blockDeletingService.shutdown();
    ContainerMetrics.remove();
  }


  @VisibleForTesting
  public ContainerSet getContainerSet() {
    return containerSet;
  }
  /**
   * Returns container report.
   * @return - container report.
   */

  public PipelineReportsProto getPipelineReport() {
    PipelineReportsProto.Builder pipelineReportsProto =
        PipelineReportsProto.newBuilder();
    pipelineReportsProto.addAllPipelineReport(writeChannel.getPipelineReport());
    return pipelineReportsProto.build();
  }

  public XceiverServerSpi getWriteChannel() {
    return writeChannel;
  }

  public XceiverServerSpi getReadChannel() {
    return readChannel;
  }

  public ContainerController getController() {
    return controller;
  }

  /**
   * Returns node report of container storage usage.
   */
  public StorageContainerDatanodeProtocolProtos.NodeReportProto getNodeReport()
      throws IOException {
    return volumeSet.getNodeReport();
  }

  @VisibleForTesting
  public ContainerDispatcher getDispatcher() {
    return this.hddsDispatcher;
  }

  public VolumeSet getVolumeSet() {
    return volumeSet;
  }
}

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

package org.apache.hadoop.ozone.container.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Helper utility to test containers.
 */
public final class ContainerTestUtils {

  private ContainerTestUtils() {
  }

  /**
   * Creates an Endpoint class for testing purpose.
   *
   * @param conf - Conf
   * @param address - InetAddres
   * @param rpcTimeout - rpcTimeOut
   * @return EndPoint
   * @throws Exception
   */
  public static EndpointStateMachine createEndpoint(Configuration conf,
      InetSocketAddress address, int rpcTimeout) throws Exception {
    RPC.setProtocolEngine(conf, StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    long version =
        RPC.getProtocolVersion(StorageContainerDatanodeProtocolPB.class);

    StorageContainerDatanodeProtocolPB rpcProxy = RPC.getProtocolProxy(
        StorageContainerDatanodeProtocolPB.class, version,
        address, UserGroupInformation.getCurrentUser(), conf,
        NetUtils.getDefaultSocketFactory(conf), rpcTimeout,
        RetryPolicies.TRY_ONCE_THEN_FAIL).getProxy();

    StorageContainerDatanodeProtocolClientSideTranslatorPB rpcClient =
        new StorageContainerDatanodeProtocolClientSideTranslatorPB(rpcProxy);
    return new EndpointStateMachine(address, rpcClient,
        new LegacyHadoopConfigurationSource(conf));
  }

  public static OzoneContainer getOzoneContainer(
      DatanodeDetails datanodeDetails, OzoneConfiguration conf)
      throws IOException {
    DatanodeStateMachine stateMachine =
        Mockito.mock(DatanodeStateMachine.class);
    Mockito.when(stateMachine.getReconfigurationHandler())
        .thenReturn(new ReconfigurationHandler("DN", conf, op -> { }));
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    return new OzoneContainer(datanodeDetails, conf, context);
  }

  public static DatanodeDetails createDatanodeDetails() {
    Random random = new Random();
    String ipAddress =
        random.nextInt(256) + "." + random.nextInt(256) + "." + random
            .nextInt(256) + "." + random.nextInt(256);

    DatanodeDetails.Port containerPort =
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort =
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort =
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  public static KeyValueContainer getContainer(long containerId,
      ContainerLayoutVersion layout,
      ContainerDataProto.State state) {
    KeyValueContainerData kvData =
        new KeyValueContainerData(containerId,
            layout,
            (long) StorageUnit.GB.toBytes(5),
            UUID.randomUUID().toString(), UUID.randomUUID().toString());
    kvData.setState(state);
    return new KeyValueContainer(kvData, new OzoneConfiguration());
  }

  public static void enableSchemaV3(OzoneConfiguration conf) {
    DatanodeConfiguration dc = conf.getObject(DatanodeConfiguration.class);
    dc.setContainerSchemaV3Enabled(true);
    conf.setFromObject(dc);
  }

  public static void disableSchemaV3(OzoneConfiguration conf) {
    DatanodeConfiguration dc = conf.getObject(DatanodeConfiguration.class);
    dc.setContainerSchemaV3Enabled(false);
    conf.setFromObject(dc);
  }

  public static void createDbInstancesForTestIfNeeded(
      MutableVolumeSet hddsVolumeSet, String scmID, String clusterID,
      ConfigurationSource conf) {
    DatanodeConfiguration dc = conf.getObject(DatanodeConfiguration.class);
    if (!dc.getContainerSchemaV3Enabled()) {
      return;
    }

    for (HddsVolume volume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      StorageVolumeUtil.checkVolume(volume, scmID, clusterID, conf,
          null, null);
    }
  }

  public static void setupMockContainer(
      Container<ContainerData> c, boolean shouldScanData,
      ScanResult metadataScanResult, ScanResult dataScanResult,
      AtomicLong containerIdSeq, HddsVolume vol) {
    ContainerData data = mock(ContainerData.class);
    when(data.getContainerID()).thenReturn(containerIdSeq.getAndIncrement());
    when(c.getContainerData()).thenReturn(data);
    when(c.shouldScanData()).thenReturn(shouldScanData);
    when(c.shouldScanMetadata()).thenReturn(true);
    when(c.getContainerData().getVolume()).thenReturn(vol);

    try {
      when(c.scanData(any(DataTransferThrottler.class), any(Canceler.class)))
          .thenReturn(dataScanResult);
      Mockito.lenient().when(c.scanMetaData()).thenReturn(metadataScanResult);
    } catch (InterruptedException ex) {
      // Mockito.when invocations will not throw this exception. It is just
      // required for compilation.
    }
  }

  /**
   * Construct an unhealthy scan result to use for testing purposes.
   */
  public static ScanResult getUnhealthyScanResult() {
    return ScanResult.unhealthy(ScanResult.FailureType.CORRUPT_CHUNK,
        new File(""),
        new IOException("Fake corruption failure for testing"));
  }

  public static KeyValueContainer addContainerToDeletedDir(
      HddsVolume volume, String clusterId,
      OzoneConfiguration conf, String schemaVersion)
      throws IOException {
    KeyValueContainer container = addContainerToVolumeDir(volume, clusterId,
        conf, schemaVersion);

    // For testing, we are moving the container
    // under the tmp directory, in order to delete
    // it from there, during datanode startup or shutdown
    KeyValueContainerUtil
        .moveToDeletedContainerDir(container.getContainerData(), volume);

    return container;
  }

  public static KeyValueContainer addContainerToVolumeDir(
      HddsVolume volume, String clusterId,
      OzoneConfiguration conf, String schemaVersion)
      throws IOException {
    long containerId = ContainerTestHelper.getTestContainerID();
    return addContainerToVolumeDir(volume, clusterId, conf, schemaVersion,
        containerId);
  }

  public static KeyValueContainer addContainerToVolumeDir(
      HddsVolume volume, String clusterId,
      OzoneConfiguration conf, String schemaVersion, long containerId)
      throws IOException {
    VolumeChoosingPolicy volumeChoosingPolicy =
        new RoundRobinVolumeChoosingPolicy();
    ContainerLayoutVersion layout = ContainerLayoutVersion.FILE_PER_BLOCK;

    KeyValueContainerData keyValueContainerData = new KeyValueContainerData(
        containerId, layout,
        ContainerTestHelper.CONTAINER_MAX_SIZE,
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    keyValueContainerData.setSchemaVersion(schemaVersion);

    KeyValueContainer container =
        new KeyValueContainer(keyValueContainerData, conf);
    container.create(volume.getVolumeSet(), volumeChoosingPolicy, clusterId);

    container.close();

    return container;
  }

  private static class NoopContainerDispatcher implements ContainerDispatcher {
    @Override
    public ContainerCommandResponseProto dispatch(
        ContainerCommandRequestProto msg, DispatcherContext context) {
      return ContainerTestHelper.getCreateContainerResponse(msg);
    }

    @Override
    public void validateContainerCommand(ContainerCommandRequestProto msg) {
    }

    @Override
    public void init() {
    }

    @Override
    public void buildMissingContainerSetAndValidate(
        Map<Long, Long> container2BCSIDMap) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Handler getHandler(ContainerType containerType) {
      return null;
    }

    @Override
    public void setClusterId(String clusterId) {
    }
  }

  private static final ContainerDispatcher NOOP_CONTAINER_DISPATCHER
      = new NoopContainerDispatcher();

  public static ContainerDispatcher getNoopContainerDispatcher() {
    return NOOP_CONTAINER_DISPATCHER;
  }

  private static final ContainerController EMPTY_CONTAINER_CONTROLLER
      = new ContainerController(new ContainerSet(1000), Collections.emptyMap());

  public static ContainerController getEmptyContainerController() {
    return EMPTY_CONTAINER_CONTROLLER;
  }

  public static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails dn, OzoneConfiguration conf) throws IOException {
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT,
        dn.getPort(DatanodeDetails.Port.Name.RATIS).getValue());

    return XceiverServerRatis.newXceiverServerRatis(dn, conf,
        getNoopContainerDispatcher(), getEmptyContainerController(),
        null, null);
  }
}

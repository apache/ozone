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

package org.apache.hadoop.ozone.container.common;

import static org.apache.hadoop.ozone.common.Storage.StorageState.INITIALIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
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
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError;
import org.apache.hadoop.ozone.container.ozoneimpl.DataScanResult;
import org.apache.hadoop.ozone.container.ozoneimpl.MetadataScanResult;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.mockito.Mockito;

/**
 * Helper utility to test containers.
 */
public final class ContainerTestUtils {

  public static final DispatcherContext WRITE_STAGE = DispatcherContext
      .newBuilder(DispatcherContext.Op.WRITE_STATE_MACHINE_DATA)
      .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
      .build();

  public static final DispatcherContext COMMIT_STAGE = DispatcherContext
      .newBuilder(DispatcherContext.Op.APPLY_TRANSACTION)
      .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA)
      .setContainer2BCSIDMap(Collections.emptyMap())
      .build();

  public static final DispatcherContext COMBINED_STAGE
      = DispatcherContext.getHandleWriteChunk();

  private static final ContainerDispatcher NOOP_CONTAINER_DISPATCHER = new NoopContainerDispatcher();

  private static final ContainerController EMPTY_CONTAINER_CONTROLLER
      = new ContainerController(ContainerImplTestUtils.newContainerSet(), Collections.emptyMap());

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
        new LegacyHadoopConfigurationSource(conf), "");
  }

  public static OzoneContainer getOzoneContainer(
      DatanodeDetails datanodeDetails, OzoneConfiguration conf)
      throws IOException {
    StateContext context = getMockContext(datanodeDetails, conf);
    VolumeChoosingPolicy volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);
    return new OzoneContainer(datanodeDetails, conf, context, volumeChoosingPolicy);
  }

  public static StateContext getMockContext(DatanodeDetails datanodeDetails,
      OzoneConfiguration conf) {
    DatanodeStateMachine stateMachine = mock(DatanodeStateMachine.class);
    Mockito.lenient().when(stateMachine.getReconfigurationHandler())
        .thenReturn(new ReconfigurationHandler("DN", conf, op -> { }));
    StateContext context = mock(StateContext.class);
    when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    when(context.getParent()).thenReturn(stateMachine);
    return context;
  }

  public static DatanodeDetails createDatanodeDetails() {
    Random random = new Random();
    String ipAddress =
        random.nextInt(256) + "." + random.nextInt(256) + "." + random
            .nextInt(256) + "." + random.nextInt(256);

    DatanodeDetails.Port containerPort =
        DatanodeDetails.newStandalonePort(0);
    DatanodeDetails.Port ratisPort =
        DatanodeDetails.newRatisPort(0);
    DatanodeDetails.Port restPort =
        DatanodeDetails.newRestPort(0);
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

  public static KeyValueHandler getKeyValueHandler(ConfigurationSource config,
      String datanodeId, ContainerSet contSet, VolumeSet volSet, ContainerMetrics metrics) {
    return getKeyValueHandler(config, datanodeId, contSet, volSet, metrics, new ContainerChecksumTreeManager(config));
  }

  /**
   * Constructs an instance of KeyValueHandler that can be used for testing.
   * This instance can be used for tests that do not need an ICR sender or {@link ContainerChecksumTreeManager}.
   */
  public static KeyValueHandler getKeyValueHandler(ConfigurationSource config,
      String datanodeId, ContainerSet contSet, VolumeSet volSet, ContainerMetrics metrics,
      ContainerChecksumTreeManager checksumTreeManager) {
    return new KeyValueHandler(config, datanodeId, contSet, volSet, metrics, c -> { }, checksumTreeManager);
  }

  /**
   * Constructs an instance of KeyValueHandler that can be used for testing.
   * This instance can be used for tests that do not need an ICR sender, metrics, or a
   * {@link ContainerChecksumTreeManager}.
   */
  public static KeyValueHandler getKeyValueHandler(ConfigurationSource config,
      String datanodeId, ContainerSet contSet, VolumeSet volSet) {
    return getKeyValueHandler(config, datanodeId, contSet, volSet, ContainerMetrics.create(config));
  }

  public static KeyValueHandler getKeyValueHandler(ConfigurationSource config,
      String datanodeId, ContainerSet contSet, VolumeSet volSet, ContainerChecksumTreeManager checksumTreeManager) {
    return getKeyValueHandler(config, datanodeId, contSet, volSet, ContainerMetrics.create(config),
        checksumTreeManager);
  }

  public static HddsDispatcher getHddsDispatcher(OzoneConfiguration conf,
                                                 ContainerSet contSet,
                                                 VolumeSet volSet,
                                                 StateContext context) {
    return getHddsDispatcher(conf, contSet, volSet, context, null);
  }

  public static HddsDispatcher getHddsDispatcher(OzoneConfiguration conf,
                                                 ContainerSet contSet,
                                                 VolumeSet volSet,
                                                 StateContext context, TokenVerifier verifier) {
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    Map<ContainerType, Handler> handlers = Maps.newHashMap();
    handlers.put(ContainerType.KeyValueContainer, ContainerTestUtils.getKeyValueHandler(conf,
        context.getParent().getDatanodeDetails().getUuidString(), contSet, volSet, metrics));
    assertEquals(1, ContainerType.values().length, "Tests only cover KeyValueContainer type");
    return new HddsDispatcher(
        conf, contSet, volSet, handlers, context, metrics, verifier);
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
      MetadataScanResult metadataScanResult, DataScanResult dataScanResult,
      AtomicLong containerIdSeq, HddsVolume vol) {
    ContainerData data = mock(ContainerData.class);
    when(data.getContainerID()).thenReturn(containerIdSeq.getAndIncrement());
    when(c.getContainerData()).thenReturn(data);
    when(c.shouldScanData()).thenReturn(shouldScanData);
    when(c.getContainerData().getVolume()).thenReturn(vol);

    try {
      when(c.scanData(any(DataTransferThrottler.class), any(Canceler.class))).thenReturn(dataScanResult);
      Mockito.lenient().when(c.scanMetaData()).thenReturn(metadataScanResult);
    } catch (InterruptedException ex) {
      // Mockito.when invocations will not throw this exception. It is just
      // required for compilation.
    }
  }

  public static DataScanResult getHealthyDataScanResult() {
    return DataScanResult.fromErrors(Collections.emptyList(), new ContainerMerkleTreeWriter());
  }

  /**
   * Construct an unhealthy scan result to use for testing purposes.
   */
  public static DataScanResult getUnhealthyDataScanResult() {
    return DataScanResult.fromErrors(Collections.singletonList(getDataScanError()), new ContainerMerkleTreeWriter());
  }

  public static MetadataScanResult getHealthyMetadataScanResult() {
    return MetadataScanResult.fromErrors(Collections.emptyList());
  }

  /**
   * Construct a generic data scan error that can be used for testing.
   */
  public static ContainerScanError getDataScanError() {
    return new ContainerScanError(ContainerScanError.FailureType.CORRUPT_CHUNK, new File(""),
        new IOException("Fake data corruption failure for testing"));
  }

  /**
   * Construct a generic metadata scan error that can be used for testing.
   */
  public static ContainerScanError getMetadataScanError() {
    return new ContainerScanError(ContainerScanError.FailureType.CORRUPT_CONTAINER_FILE, new File(""),
        new IOException("Fake metadata corruption failure for testing"));
  }

  /**
   * Construct an unhealthy scan result to use for testing purposes.
   */
  public static MetadataScanResult getUnhealthyMetadataScanResult() {
    return DataScanResult.fromErrors(Collections.singletonList(getMetadataScanError()));
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

  public static ContainerDispatcher getNoopContainerDispatcher() {
    return NOOP_CONTAINER_DISPATCHER;
  }

  public static ContainerController getEmptyContainerController() {
    return EMPTY_CONTAINER_CONTROLLER;
  }

  public static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails dn, OzoneConfiguration conf) throws IOException {
    conf.setInt(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_PORT,
        dn.getRatisPort().getValue());

    return XceiverServerRatis.newXceiverServerRatis(null, dn, conf,
        getNoopContainerDispatcher(), getEmptyContainerController(),
        null, null);
  }

  /** Initialize {@link DatanodeLayoutStorage}.  Normally this is done during {@link HddsDatanodeService} start,
   * have to do the same for tests that create {@link OzoneContainer} manually. */
  public static void initializeDatanodeLayout(ConfigurationSource conf, DatanodeDetails dn) throws IOException {
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf, dn.getUuidString());
    if (layoutStorage.getState() != INITIALIZED) {
      layoutStorage.initialize();
    }
  }

  /**
   * Creates block metadata for the given container with the specified number of blocks and chunks per block.
   */
  public static void createBlockMetaData(KeyValueContainerData data, int numOfBlocksPerContainer,
                                         int numOfChunksPerBlock) throws IOException {
    try (DBHandle metadata = BlockUtils.getDB(data, new OzoneConfiguration())) {
      for (int j = 0; j < numOfBlocksPerContainer; j++) {
        BlockID blockID = new BlockID(data.getContainerID(), j);
        String blockKey = data.getBlockKey(blockID.getLocalID());
        BlockData kd = new BlockData(blockID);
        List<ContainerProtos.ChunkInfo> chunks = Lists.newArrayList();
        for (int k = 0; k < numOfChunksPerBlock; k++) {
          long dataLen = 10L;
          ChunkInfo chunkInfo = ContainerTestHelper.getChunk(blockID.getLocalID(), k, k * dataLen, dataLen);
          ContainerTestHelper.setDataChecksum(chunkInfo, ContainerTestHelper.getData((int) dataLen));
          chunks.add(chunkInfo.getProtoBufMessage());
        }
        kd.setChunks(chunks);
        metadata.getStore().getBlockDataTable().put(blockKey, kd);
      }
    }
  }
}

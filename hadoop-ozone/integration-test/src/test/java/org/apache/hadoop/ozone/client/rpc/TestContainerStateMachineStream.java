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

package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.container.OzoneTestHelper.createStreamKey;
import static org.apache.hadoop.ozone.container.OzoneTestHelper.getDatanodeService;
import static org.apache.hadoop.ozone.container.OzoneTestHelper.getDatanodeServices;
import static org.apache.hadoop.ozone.container.OzoneTestHelper.validateData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the containerStateMachine stream handling.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestContainerStateMachineStream implements NonHATests.TestCase {
  private OzoneClient client;
  private String volumeName;
  private String bucketName;
  private int chunkSize;

  @BeforeAll
  void setup() throws Exception {
    chunkSize = (int) cluster().getConf().getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, 1024 * 1024, StorageUnit.BYTES);

    client = cluster().newClient();
    ObjectStore objectStore = client.getObjectStore();

    volumeName = "vol-" + UUID.randomUUID();
    bucketName = "teststreambucket";
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(client);
  }

  private static Stream<Arguments> streamingParameters() {
    return Stream.of(-1, +1).flatMap(offset ->
        Stream.of(false, true).map(putBlockWithoutRaft ->
            Arguments.of(offset, putBlockWithoutRaft)));
  }

  @ParameterizedTest
  @MethodSource("streamingParameters")
  void testContainerStateMachineForStreaming(int offset, boolean putBlockWithoutRaft)
      throws Exception {
    final int size = chunkSize + offset;
    OzoneConfiguration conf = new OzoneConfiguration(cluster().getConf());
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setDatastreamPutBlockWithoutRaftEnabled(putBlockWithoutRaft);
    conf.setFromObject(clientConfig);

    final List<OmKeyLocationInfo> locationInfoList;
    try (OzoneClient streamingClient = OzoneClientFactory.getRpcClient(conf);
         OzoneDataStreamOutput key = createStreamKey("key" + offset + "-" + putBlockWithoutRaft,
             ReplicationType.RATIS, size, streamingClient.getObjectStore(), volumeName, bucketName)) {

      byte[] data = ContainerTestHelper.generateData(size, true);
      key.write(ByteBuffer.wrap(data));
      key.flush();

      locationInfoList = assertInstanceOf(KeyDataStreamOutput.class, key.getByteBufStreamOutput())
          .getLocationInfoList();
    }

    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);

    long bytesUsed = getDatanodeService(omKeyLocationInfo, cluster())
        .getDatanodeStateMachine()
        .getContainer()
        .getContainerSet()
        .getContainer(omKeyLocationInfo.getContainerID())
        .getContainerData()
        .getBytesUsed();

    assertThat(bytesUsed)
        // container may have previous data
        .isGreaterThanOrEqualTo(size);
  }

  @Test
  void testReadAfterHsyncWithPutBlockWithoutRaft() throws Exception {
    // Smaller than PUT_BLOCK_REQUEST_LENGTH_MAX, so the datanodes still buffer all the data when hsync commits PutBlock
    final int size = 1000;
    OzoneConfiguration conf = new OzoneConfiguration(cluster().getConf());
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setDatastreamPutBlockWithoutRaftEnabled(true);
    conf.setFromObject(clientConfig);

    final String keyName = "hsync-" + UUID.randomUUID();
    try (OzoneClient streamingClient = OzoneClientFactory.getRpcClient(conf);
         OzoneDataStreamOutput key = createStreamKey(keyName, ReplicationType.RATIS, size,
             streamingClient.getObjectStore(), volumeName, bucketName)) {
      final byte[] data = ContainerTestHelper.generateData(size, true);
      key.write(ByteBuffer.wrap(data));
      assertInstanceOf(KeyDataStreamOutput.class, key.getByteBufStreamOutput()).hsync();

      validateData(keyName, data, client.getObjectStore(), volumeName, bucketName);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testPutBlockWithoutRaftCommittedOnAllDatanodes(boolean pipelineMode) throws Exception {
    final int size = 1000;
    OzoneConfiguration conf = new OzoneConfiguration(cluster().getConf());
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setDatastreamPutBlockWithoutRaftEnabled(true);
    clientConfig.setDatastreamPipelineMode(pipelineMode);
    conf.setFromObject(clientConfig);

    final String keyName = "all-dn-" + pipelineMode + "-" + UUID.randomUUID();
    final byte[] data = ContainerTestHelper.generateData(2 * size, true);
    final OmKeyLocationInfo info;
    try (OzoneClient streamingClient = OzoneClientFactory.getRpcClient(conf);
         OzoneDataStreamOutput key = createStreamKey(keyName, ReplicationType.RATIS, 2 * size,
             streamingClient.getObjectStore(), volumeName, bucketName)) {
      final KeyDataStreamOutput out = assertInstanceOf(KeyDataStreamOutput.class, key.getByteBufStreamOutput());
      key.write(ByteBuffer.wrap(data, 0, size));
      out.hsync();
      info = out.getLocationInfoList().get(0);
      // hsync returns only after the PutBlock is committed on every datanode of the pipeline
      assertCommittedOnAllDatanodes(info, size);

      key.write(ByteBuffer.wrap(data, size, size));
    }
    assertCommittedOnAllDatanodes(info, 2 * size);
    validateData(keyName, data, client.getObjectStore(), volumeName, bucketName);

    // The log index of the Raft log entry closing the stream is used as the BCSID
    final OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).build();
    final long bcsId = cluster().getOzoneManager().lookupKey(keyArgs).getLatestVersionLocations()
        .getBlocksLatestVersionOnly().get(0).getBlockID().getBlockCommitSequenceId();
    assertThat(bcsId).isPositive();
    // The followers may apply the entry after the client is acknowledged
    for (HddsDatanodeService dn : getDatanodeServices(cluster(), info.getPipeline())) {
      GenericTestUtils.waitFor(() -> getBlockData(dn, info).getBlockCommitSequenceId() == bcsId, 100, 10_000);
    }
  }

  private void assertCommittedOnAllDatanodes(OmKeyLocationInfo info, long expectedLength) {
    final Set<HddsDatanodeService> datanodes = getDatanodeServices(cluster(), info.getPipeline());
    for (HddsDatanodeService dn : datanodes) {
      assertEquals(expectedLength, getBlockData(dn, info).getSize(),
          () -> "Unexpected committed block length on " + dn.getDatanodeDetails());
    }
  }

  private static BlockData getBlockData(HddsDatanodeService dn, OmKeyLocationInfo info) {
    final OzoneContainer ozoneContainer = dn.getDatanodeStateMachine().getContainer();
    final KeyValueHandler handler = (KeyValueHandler) ozoneContainer.getDispatcher()
        .getHandler(ContainerProtos.ContainerType.KeyValueContainer);
    final Container<?> container = ozoneContainer.getContainerSet().getContainer(info.getContainerID());
    try {
      // Without BCSID so that the BCSID is not verified
      return handler.getBlockManager().getBlock(container, new BlockID(info.getContainerID(), info.getLocalID()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
        Stream.of(false, true).map(putBlockOnCloseEnabled ->
            Arguments.of(offset, putBlockOnCloseEnabled)));
  }

  @ParameterizedTest
  @MethodSource("streamingParameters")
  void testContainerStateMachineForStreaming(int offset, boolean putBlockOnCloseEnabled)
      throws Exception {
    final int size = chunkSize + offset;
    OzoneConfiguration conf = new OzoneConfiguration(cluster().getConf());
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setDatastreamPutBlockOnCloseEnabled(putBlockOnCloseEnabled);
    conf.setFromObject(clientConfig);

    final List<OmKeyLocationInfo> locationInfoList;
    try (OzoneClient streamingClient = OzoneClientFactory.getRpcClient(conf);
         OzoneDataStreamOutput key = createStreamKey("key" + offset + "-" + putBlockOnCloseEnabled,
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

}

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
import static org.apache.hadoop.ozone.container.TestHelper.createStreamKey;
import static org.apache.hadoop.ozone.container.TestHelper.getDatanodeService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.KeyDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the containerStateMachine stream handling.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestContainerStateMachineStream implements NonHATests.TestCase {
  private OzoneClient client;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private int chunkSize;

  @BeforeAll
  void setup() throws Exception {
    chunkSize = (int) cluster().getConf().getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, 1024 * 1024, StorageUnit.BYTES);

    client = cluster().newClient();
    objectStore = client.getObjectStore();

    volumeName = "vol-" + UUID.randomUUID();
    bucketName = "teststreambucket";
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(client);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, +1})
  void testContainerStateMachineForStreaming(int offset) throws Exception {
    final int size = chunkSize + offset;

    final List<OmKeyLocationInfo> locationInfoList;
    try (OzoneDataStreamOutput key = createStreamKey("key" + offset, ReplicationType.RATIS, size,
        objectStore, volumeName, bucketName)) {

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

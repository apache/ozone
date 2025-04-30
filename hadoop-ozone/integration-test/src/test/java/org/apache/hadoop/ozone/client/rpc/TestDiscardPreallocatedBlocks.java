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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.container.TestHelper.createKey;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerClose;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests that unused block pre-allocated for write is discarded when container is closed.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestDiscardPreallocatedBlocks implements NonHATests.TestCase {
  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore objectStore;
  private int blockSize;
  private String volumeName;
  private String bucketName;
  private String keyString;

  @BeforeAll
  void init() throws Exception {
    blockSize = (int) cluster().getConf().getStorageSize(OZONE_SCM_BLOCK_SIZE,
        OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);

    cluster = cluster();
    client = cluster.newClient();
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = UUID.randomUUID().toString();
    bucketName = "bucket";
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
  }

  @Test
  public void testDiscardPreallocatedBlocks() throws Exception {
    String keyName = "key";
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, 2L * blockSize, objectStore, volumeName, bucketName);
    KeyOutputStream keyOutputStream =
        assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    // With the initial size provided, it should have pre allocated 2 blocks
    assertEquals(2, keyOutputStream.getStreamEntries().size());
    String dataString =
        ContainerTestHelper.getFixedLengthString(keyString, (1 * blockSize));
    byte[] data = dataString.getBytes(UTF_8);
    key.write(data);
    List<OmKeyLocationInfo> locationInfos =
        new ArrayList<>(keyOutputStream.getLocationInfoList());
    List<BlockOutputStreamEntry> locationStreamInfos =
        new ArrayList<>(keyOutputStream.getStreamEntries());
    long containerID = locationInfos.get(0).getContainerID();
    ContainerInfo container =
        cluster.getStorageContainerManager().getContainerManager()
            .getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    assertEquals(3, datanodes.size());
    waitForContainerClose(key, cluster);
    dataString =
        ContainerTestHelper.getFixedLengthString(keyString, (1 * blockSize));
    data = dataString.getBytes(UTF_8);
    key.write(data);
    assertEquals(3, keyOutputStream.getStreamEntries().size());
    // the 1st block got written. Now all the containers are closed, so the 2nd
    // pre allocated block will be removed from the list and new block should
    // have been allocated
    assertEquals(
        keyOutputStream.getLocationInfoList().get(0).getBlockID(),
        locationInfos.get(0).getBlockID());
    assertNotEquals(locationStreamInfos.get(1).getBlockID(),
        keyOutputStream.getLocationInfoList().get(1).getBlockID());
    key.close();
  }

}

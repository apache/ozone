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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests Close Container Exception handling by Ozone Client.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestCloseContainerHandlingByClient implements NonHATests.TestCase {

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore objectStore;
  private int chunkSize;
  private int blockSize;
  private String volumeName;
  private String bucketName;
  private String keyString;

  @BeforeAll
  void init() throws Exception {
    chunkSize = (int) cluster().getConf().getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, 1024 * 1024, StorageUnit.BYTES);
    blockSize = (int) cluster().getConf().getStorageSize(OZONE_SCM_BLOCK_SIZE, 4 * chunkSize, StorageUnit.BYTES);

    cluster = cluster();
    client = cluster().newClient();
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "vol-" + UUID.randomUUID();
    bucketName = "bucket";
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  private String getKeyName() {
    return UUID.randomUUID().toString();
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
  }

  @Test
  public void testBlockWritesWithFlushAndClose() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    // write data more than 1 chunk
    byte[] data = ContainerTestHelper
        .getFixedLengthString(keyString, chunkSize + chunkSize / 2)
        .getBytes(UTF_8);
    key.write(data);

    assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setKeyName(keyName)
        .build();

    waitForContainerClose(key);
    key.write(data);
    key.flush();
    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    assertEquals(2 * data.length, keyInfo.getDataSize());

    // Written the same data twice
    String dataString = new String(data, UTF_8);
    dataString = dataString.concat(dataString);
    validateData(keyName, dataString.getBytes(UTF_8));
  }

  @Test
  public void testBlockWritesCloseConsistency() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    // write data more than 1 chunk
    byte[] data = ContainerTestHelper
        .getFixedLengthString(keyString, chunkSize + chunkSize / 2)
        .getBytes(UTF_8);
    key.write(data);

    assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setKeyName(keyName)
        .build();

    waitForContainerClose(key);
    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    assertEquals(data.length, keyInfo.getDataSize());
    validateData(keyName, data);
  }

  @Test
  public void testMultiBlockWrites() throws Exception {

    String keyName = getKeyName();
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, (3 * blockSize));
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) key.getOutputStream();
    // With the initial size provided, it should have preallocated 4 blocks
    assertEquals(3, keyOutputStream.getStreamEntries().size());
    // write data more than 1 block
    byte[] data =
        ContainerTestHelper.getFixedLengthString(keyString, (3 * blockSize))
            .getBytes(UTF_8);
    assertEquals(data.length, 3 * blockSize);
    key.write(data);

    assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setKeyName(keyName)
        .build();

    waitForContainerClose(key);
    // write 1 more block worth of data. It will fail and new block will be
    // allocated
    key.write(ContainerTestHelper.getFixedLengthString(keyString, blockSize)
        .getBytes(UTF_8));

    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    List<OmKeyLocationInfo> keyLocationInfos =
        keyInfo.getKeyLocationVersions().get(0).getBlocksLatestVersionOnly();
    // Though we have written only block initially, the close will hit
    // closeContainerException and remaining data in the chunkOutputStream
    // buffer will be copied into a different allocated block and will be
    // committed.
    assertEquals(4, keyLocationInfos.size());
    assertEquals(4 * blockSize, keyInfo.getDataSize());
    for (OmKeyLocationInfo locationInfo : keyLocationInfos) {
      assertEquals(blockSize, locationInfo.getLength());
    }
  }

  @Test
  public void testMultiBlockWrites2() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, 2 * blockSize);
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) key.getOutputStream();

    assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    // With the initial size provided, it should have pre allocated 2 blocks
    assertEquals(2, keyOutputStream.getStreamEntries().size());
    String dataString =
        ContainerTestHelper.getFixedLengthString(keyString, (2 * blockSize));
    byte[] data = dataString.getBytes(UTF_8);
    key.write(data);
    // 2 block are completely written to the DataNode in 3 blocks.
    // Data of length half of chunkSize resides in the chunkOutput stream buffer
    String dataString2 =
        ContainerTestHelper.getFixedLengthString(keyString, chunkSize);
    key.write(dataString2.getBytes(UTF_8));
    key.flush();

    String dataString3 =
        ContainerTestHelper.getFixedLengthString(keyString, chunkSize);
    key.write(dataString3.getBytes(UTF_8));
    key.flush();

    String dataString4 =
        ContainerTestHelper.getFixedLengthString(keyString, chunkSize * 1 / 2);
    key.write(dataString4.getBytes(UTF_8));
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName)
        .build();

    waitForContainerClose(key);

    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    // Though we have written only block initially, the close will hit
    // closeContainerException and remaining data in the chunkOutputStream
    // buffer will be copied into a different allocated block and will be
    // committed.

    String dataCommitted =
        dataString.concat(dataString2).concat(dataString3).concat(dataString4);
    assertEquals(dataCommitted.getBytes(UTF_8).length,
        keyInfo.getDataSize());
    validateData(keyName, dataCommitted.getBytes(UTF_8));
  }

  @Test
  public void testMultiBlockWrites3() throws Exception {

    String keyName = getKeyName();
    int keyLen = 4 * blockSize;
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, keyLen);
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) key.getOutputStream();
    // With the initial size provided, it should have preallocated 4 blocks
    assertEquals(4, keyOutputStream.getStreamEntries().size());
    // write data 4 blocks and one more chunk
    byte[] writtenData =
        ContainerTestHelper.getFixedLengthString(keyString, keyLen)
            .getBytes(UTF_8);
    byte[] data = Arrays.copyOfRange(writtenData, 0, 3 * blockSize + chunkSize);
    assertEquals(data.length, 3 * blockSize + chunkSize);
    key.write(data);

    assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName)
        .build();

    waitForContainerClose(key);
    // write 3 more chunks worth of data. It will fail and new block will be
    // allocated. This write completes 4 blocks worth of data written to key
    data = Arrays.copyOfRange(writtenData, 3 * blockSize + chunkSize, keyLen);
    key.write(data);

    key.close();
    // read the key from OM again and match the length and data.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    List<OmKeyLocationInfo> keyLocationInfos =
        keyInfo.getKeyLocationVersions().get(0).getBlocksLatestVersionOnly();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    byte[] readData = new byte[keyLen];
    try (OzoneInputStream inputStream = bucket.readKey(keyName)) {
      IOUtils.readFully(inputStream, readData);
    }
    assertArrayEquals(writtenData, readData);

    // Though we have written only block initially, the close will hit
    // closeContainerException and remaining data in the chunkOutputStream
    // buffer will be copied into a different allocated block and will be
    // committed.
    long length = 0;
    for (OmKeyLocationInfo locationInfo : keyLocationInfos) {
      length += locationInfo.getLength();
    }
    assertEquals(4 * blockSize, length);
  }

  private void waitForContainerClose(OzoneOutputStream outputStream)
      throws Exception {
    TestHelper
        .waitForContainerClose(outputStream, cluster);
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
                                      long size) throws Exception {
    return TestHelper
        .createKey(keyName, type, size, objectStore, volumeName, bucketName);
  }

  private void validateData(String keyName, byte[] data) throws Exception {
    TestHelper
        .validateData(keyName, data, objectStore, volumeName, bucketName);
  }

  @Test
  public void testBlockWriteViaRatis() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    byte[] data = ContainerTestHelper
        .getFixedLengthString(keyString, chunkSize + chunkSize / 2)
        .getBytes(UTF_8);
    key.write(data);

    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName).
        setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName)
        .build();

    assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    waitForContainerClose(key);
    // Again Write the Data. This will throw an exception which will be handled
    // and new blocks will be allocated
    key.write(data);
    key.flush();
    // The write will fail but exception will be handled and length will be
    // updated correctly in OzoneManager once the steam is closed
    key.close();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    String dataString = new String(data, UTF_8);
    dataString = dataString.concat(dataString);
    assertEquals(2 * data.length, keyInfo.getDataSize());
    validateData(keyName, dataString.getBytes(UTF_8));
  }

  @Test
  public void testBlockWrites() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    // write data more than 1 chunk
    byte[] data1 =
        ContainerTestHelper.getFixedLengthString(keyString, 2 * chunkSize)
            .getBytes(UTF_8);
    key.write(data1);

    assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setKeyName(keyName)
        .build();

    waitForContainerClose(key);
    byte[] data2 =
        ContainerTestHelper.getFixedLengthString(keyString, 3 * chunkSize)
            .getBytes(UTF_8);
    key.write(data2);
    key.flush();
    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    assertEquals((long) 5 * chunkSize, keyInfo.getDataSize());

    // Written the same data twice
    String dataString = new String(data1, UTF_8);
    // Written the same data twice
    String dataString2 = new String(data2, UTF_8);
    dataString = dataString.concat(dataString2);
    validateData(keyName, dataString.getBytes(UTF_8));
  }

}

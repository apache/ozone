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

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.hdds.scm.container.common.helpers.
    StorageContainerException;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.ChunkGroupOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Tests Close Container Exception handling by Ozone Client.
 */
public class TestCloseContainerHandlingByClient {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static int chunkSize;
  private static int blockSize;
  private static String volumeName;
  private static String bucketName;
  private static String keyString;
  private static int maxRetries;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    maxRetries = 100;
    conf.setInt(OzoneConfigKeys.OZONE_CLIENT_MAX_RETRIES, maxRetries);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_RETRY_INTERVAL, "200ms");
    chunkSize = (int) OzoneConsts.MB;
    blockSize = 4 * chunkSize;
    conf.setInt(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY, chunkSize);
    conf.setLong(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_IN_MB, (4));
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
    keyString = UUID.randomUUID().toString();
    volumeName = "closecontainerexceptionhandlingtest";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static String fixedLengthString(String string, int length) {
    return String.format("%1$" + length + "s", string);
  }

  @Test
  public void testBlockWritesWithFlushAndClose() throws Exception {
    String keyName = "standalone";
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.STAND_ALONE, 0);
    // write data more than 1 chunk
    byte[] data =
        fixedLengthString(keyString, chunkSize + chunkSize / 2).getBytes();
    key.write(data);

    Assert.assertTrue(key.getOutputStream() instanceof ChunkGroupOutputStream);
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName(keyName)
        .build();

    waitForContainerClose(keyName, key, HddsProtos.ReplicationType.STAND_ALONE);
    key.write(data);
    key.flush();
    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    List<OmKeyLocationInfo> keyLocationInfos =
        keyInfo.getKeyLocationVersions().get(0).getBlocksLatestVersionOnly();
    //we have written two blocks
    Assert.assertEquals(2, keyLocationInfos.size());
    OmKeyLocationInfo omKeyLocationInfo = keyLocationInfos.get(0);
    Assert.assertEquals(data.length - (data.length % chunkSize),
        omKeyLocationInfo.getLength());
    Assert.assertEquals(data.length + (data.length % chunkSize),
        keyLocationInfos.get(1).getLength());
    Assert.assertEquals(2 * data.length, keyInfo.getDataSize());

    // Written the same data twice
    String dataString = new String(data);
    dataString.concat(dataString);
    validateData(keyName, dataString.getBytes());
  }

  @Test
  public void testBlockWritesCloseConsistency() throws Exception {
    String keyName = "standalone2";
    OzoneOutputStream key = createKey(keyName, ReplicationType.STAND_ALONE, 0);
    // write data more than 1 chunk
    byte[] data =
        fixedLengthString(keyString, chunkSize + chunkSize / 2).getBytes();
    key.write(data);

    Assert.assertTrue(key.getOutputStream() instanceof ChunkGroupOutputStream);
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName(keyName)
        .build();

    waitForContainerClose(keyName, key, HddsProtos.ReplicationType.STAND_ALONE);
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
    Assert.assertEquals(2, keyLocationInfos.size());
    OmKeyLocationInfo omKeyLocationInfo = keyLocationInfos.get(0);
    Assert.assertEquals(data.length - (data.length % chunkSize),
        omKeyLocationInfo.getLength());
    Assert.assertEquals(data.length % chunkSize,
        keyLocationInfos.get(1).getLength());
    Assert.assertEquals(data.length, keyInfo.getDataSize());
    validateData(keyName, data);
  }

  @Test
  public void testMultiBlockWrites() throws Exception {

    String keyName = "standalone3";
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.STAND_ALONE, (4 * blockSize));
    ChunkGroupOutputStream groupOutputStream =
        (ChunkGroupOutputStream) key.getOutputStream();
    // With the initial size provided, it should have preallocated 4 blocks
    Assert.assertEquals(4, groupOutputStream.getStreamEntries().size());
    // write data for 3 blocks and 1 more chunk
    byte[] data = fixedLengthString(keyString, (3 * blockSize)).getBytes();
    Assert.assertEquals(data.length, 3 * blockSize);
    key.write(data);

    Assert.assertTrue(key.getOutputStream() instanceof ChunkGroupOutputStream);
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName(keyName)
        .build();

    waitForContainerClose(keyName, key,
        HddsProtos.ReplicationType.STAND_ALONE);
    // write 1 more block worth of data. It will fail and new block will be
    // allocated
    key.write(fixedLengthString(keyString, blockSize).getBytes());

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
    Assert.assertEquals(4, keyLocationInfos.size());
    Assert.assertEquals(4 * blockSize, keyInfo.getDataSize());
    for (OmKeyLocationInfo locationInfo : keyLocationInfos) {
      Assert.assertEquals(blockSize, locationInfo.getLength());
    }
  }

  @Test
  public void testMultiBlockWrites2() throws Exception {
    String keyName = "standalone4";
    long dataLength;
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.STAND_ALONE, 4 * blockSize);
    ChunkGroupOutputStream groupOutputStream =
        (ChunkGroupOutputStream) key.getOutputStream();

    Assert.assertTrue(key.getOutputStream() instanceof ChunkGroupOutputStream);
    // With the initial size provided, it should have pre allocated 4 blocks
    Assert.assertEquals(4, groupOutputStream.getStreamEntries().size());
    String dataString =
        fixedLengthString(keyString, (3 * blockSize + chunkSize));
    byte[] data = dataString.getBytes();
    key.write(data);
    // 3 block are completely written to the DataNode in 3 blocks.
    // Data of length half of chunkSize resides in the chunkOutput stream buffer
    String dataString2 = fixedLengthString(keyString, chunkSize * 1 / 2);
    key.write(dataString2.getBytes());
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName(keyName)
        .build();

    waitForContainerClose(keyName, key, HddsProtos.ReplicationType.STAND_ALONE);

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
    Assert.assertEquals(5, keyLocationInfos.size());
    dataLength = 3 * blockSize + (long) (1.5 * chunkSize);
    Assert.assertEquals(dataLength, keyInfo.getDataSize());
    validateData(keyName, dataString.concat(dataString2).getBytes());
  }

  @Test
  public void testMultiBlockWrites3() throws Exception {

    String keyName = "standalone5";
    int keyLen = 4 * blockSize;
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.RATIS, keyLen);
    ChunkGroupOutputStream groupOutputStream =
        (ChunkGroupOutputStream) key.getOutputStream();
    // With the initial size provided, it should have preallocated 4 blocks
    Assert.assertEquals(4, groupOutputStream.getStreamEntries().size());
    // write data 3 blocks and one more chunk
    byte[] writtenData = fixedLengthString(keyString, keyLen).getBytes();
    byte[] data = Arrays.copyOfRange(writtenData, 0, 3 * blockSize + chunkSize);
    Assert.assertEquals(data.length, 3 * blockSize + chunkSize);
    key.write(data);

    Assert.assertTrue(key.getOutputStream() instanceof ChunkGroupOutputStream);
    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName(keyName)
        .build();

    waitForContainerClose(keyName, key,
        HddsProtos.ReplicationType.RATIS);
    // write 3 more chunks worth of data. It will fail and new block will be
    // allocated. This write completes 4 blocks worth of data written to key
    data = Arrays
        .copyOfRange(writtenData, 3 * blockSize + chunkSize, keyLen);
    key.write(data);

    key.close();
    // read the key from OM again and match the length and data.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    List<OmKeyLocationInfo> keyLocationInfos =
        keyInfo.getKeyLocationVersions().get(0).getBlocksLatestVersionOnly();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OzoneInputStream inputStream = bucket.readKey(keyName);
    byte[] readData = new byte[keyLen];
    inputStream.read(readData);
    Assert.assertArrayEquals(writtenData, readData);

    // Though we have written only block initially, the close will hit
    // closeContainerException and remaining data in the chunkOutputStream
    // buffer will be copied into a different allocated block and will be
    // committed.
    Assert.assertEquals(5, keyLocationInfos.size());
    Assert.assertEquals(4 * blockSize, keyInfo.getDataSize());
    long length = 0;
    for (OmKeyLocationInfo locationInfo : keyLocationInfos) {
      length += locationInfo.getLength();
    }
    Assert.assertEquals(4 * blockSize, length);
  }

  private void waitForContainerClose(String keyName,
      OzoneOutputStream outputStream, HddsProtos.ReplicationType type)
      throws Exception {
    ChunkGroupOutputStream groupOutputStream =
        (ChunkGroupOutputStream) outputStream.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    List<Long> containerIdList = new ArrayList<>();
    for (OmKeyLocationInfo info : locationInfoList) {
      containerIdList.add(info.getContainerID());
    }
    Assert.assertTrue(!containerIdList.isEmpty());
    waitForContainerClose(type, containerIdList.toArray(new Long[0]));
  }

  private void waitForContainerClose(HddsProtos.ReplicationType type,
      Long... containerIdList)
      throws ContainerNotFoundException, PipelineNotFoundException,
      TimeoutException, InterruptedException {
    List<Pipeline> pipelineList = new ArrayList<>();
    for (long containerID : containerIdList) {
      cluster.getStorageContainerManager().getEventQueue()
          .fireEvent(SCMEvents.CLOSE_CONTAINER,
              ContainerID.valueof(containerID));
      Pipeline pipeline =
          cluster.getStorageContainerManager().getContainerManager()
              .getContainerWithPipeline(ContainerID.valueof(containerID))
              .getPipeline();
      pipelineList.add(pipeline);
      List<DatanodeDetails> datanodes = pipeline.getNodes();
      for (DatanodeDetails details : datanodes) {
        Assert.assertFalse(ContainerTestHelper
            .isContainerClosed(cluster, containerID, details));
        // send the order to close the container
        cluster.getStorageContainerManager().getScmNodeManager()
            .addDatanodeCommand(details.getUuid(),
                new CloseContainerCommand(containerID, type, pipeline.getId()));
      }
    }
    int index = 0;
    for (long containerID : containerIdList) {
      Pipeline pipeline = pipelineList.get(index);
      List<DatanodeDetails> datanodes = pipeline.getNodes();
      // Below condition avoids the case where container has been allocated
      // but not yet been used by the client. In such a case container is never
      // created.
      if (datanodes.stream().anyMatch(dn -> ContainerTestHelper
          .isContainerPresent(cluster, containerID, dn))) {
        for (DatanodeDetails datanodeDetails : datanodes) {
          GenericTestUtils.waitFor(() -> ContainerTestHelper
                  .isContainerClosed(cluster, containerID, datanodeDetails),
              500, 15 * 1000);
          //double check if it's really closed
          // (waitFor also throws an exception)
          Assert.assertTrue(ContainerTestHelper
              .isContainerClosed(cluster, containerID, datanodeDetails));
        }
      }
      index++;
    }
  }

  @Ignore // test needs to be fixed after close container is handled for
  // non-existent containers on datanode. Test closes pre allocated containers
  // on the datanode.
  @Test
  public void testDiscardPreallocatedBlocks() throws Exception {
    String keyName = "discardpreallocatedblocks";
    OzoneOutputStream key =
        createKey(keyName, ReplicationType.STAND_ALONE, 2 * blockSize);
    ChunkGroupOutputStream groupOutputStream =
        (ChunkGroupOutputStream) key.getOutputStream();

    Assert.assertTrue(key.getOutputStream() instanceof ChunkGroupOutputStream);
    // With the initial size provided, it should have pre allocated 4 blocks
    Assert.assertEquals(2, groupOutputStream.getStreamEntries().size());
    String dataString = fixedLengthString(keyString, (1 * blockSize));
    byte[] data = dataString.getBytes();
    key.write(data);
    List<OmKeyLocationInfo> locationInfos =
        new ArrayList<>(groupOutputStream.getLocationInfoList());
    long containerID = locationInfos.get(0).getContainerID();
    List<DatanodeDetails> datanodes =
        cluster.getStorageContainerManager().getContainerManager()
            .getContainerWithPipeline(ContainerID.valueof(containerID))
            .getPipeline().getNodes();
    Assert.assertEquals(1, datanodes.size());
    waitForContainerClose(keyName, key, HddsProtos.ReplicationType.STAND_ALONE);
    dataString = fixedLengthString(keyString, (1 * blockSize));
    data = dataString.getBytes();
    key.write(data);
    Assert.assertEquals(2, groupOutputStream.getStreamEntries().size());

    // the 1st block got written. Now all the containers are closed, so the 2nd
    // pre allocated block will be removed from the list and new block should
    // have been allocated
    Assert.assertTrue(
        groupOutputStream.getLocationInfoList().get(0).getBlockID()
            .equals(locationInfos.get(0).getBlockID()));
    Assert.assertFalse(
        groupOutputStream.getLocationInfoList().get(1).getBlockID()
            .equals(locationInfos.get(1).getBlockID()));
    key.close();
  }

  private OzoneOutputStream createKey(String keyName, ReplicationType type,
      long size) throws Exception {
    ReplicationFactor factor =
        type == ReplicationType.STAND_ALONE ? ReplicationFactor.ONE :
            ReplicationFactor.THREE;
    return objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey(keyName, size, type, factor);
  }

  private void validateData(String keyName, byte[] data) throws Exception {
    byte[] readData = new byte[data.length];
    OzoneInputStream is =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .readKey(keyName);
    is.read(readData);
    MessageDigest sha1 = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    sha1.update(data);
    MessageDigest sha2 = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    sha2.update(readData);
    Assert.assertTrue(Arrays.equals(sha1.digest(), sha2.digest()));
    is.close();
  }

  @Test
  public void testBlockWriteViaRatis() throws Exception {
    String keyName = "ratis";
    OzoneOutputStream key = createKey(keyName, ReplicationType.RATIS, 0);
    byte[] data =
        fixedLengthString(keyString, chunkSize + chunkSize / 2).getBytes();
    key.write(data);

    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName).
        setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.THREE)
        .setKeyName(keyName).build();

    Assert.assertTrue(key.getOutputStream() instanceof ChunkGroupOutputStream);
    waitForContainerClose(keyName, key, HddsProtos.ReplicationType.RATIS);
    // Again Write the Data. This will throw an exception which will be handled
    // and new blocks will be allocated
    key.write(data);
    key.flush();
    // The write will fail but exception will be handled and length will be
    // updated correctly in OzoneManager once the steam is closed
    key.close();
    // read the key from OM again and match the length.The length will still
    // be the equal to the original data size.
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    List<OmKeyLocationInfo> keyLocationInfos =
        keyInfo.getKeyLocationVersions().get(0).getBlocksLatestVersionOnly();
    //we have written two blocks
    Assert.assertEquals(2, keyLocationInfos.size());
    OmKeyLocationInfo omKeyLocationInfo = keyLocationInfos.get(0);
    Assert.assertEquals(data.length - (data.length % chunkSize),
        omKeyLocationInfo.getLength());
    Assert.assertEquals(data.length + (data.length % chunkSize),
        keyLocationInfos.get(1).getLength());
    Assert.assertEquals(2 * data.length, keyInfo.getDataSize());
    String dataString = new String(data);
    dataString.concat(dataString);
    validateData(keyName, dataString.getBytes());
  }

  @Test
  public void testRetriesOnBlockNotCommittedException() throws Exception {
    String keyName = "blockcommitexceptiontest";
    OzoneOutputStream key = createKey(keyName, ReplicationType.STAND_ALONE, 0);
    ChunkGroupOutputStream groupOutputStream =
        (ChunkGroupOutputStream) key.getOutputStream();
    GenericTestUtils.setLogLevel(ChunkGroupOutputStream.LOG, Level.TRACE);
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(ChunkGroupOutputStream.LOG);

    Assert.assertTrue(key.getOutputStream() instanceof ChunkGroupOutputStream);
    String dataString = fixedLengthString(keyString, (3 * chunkSize));
    key.write(dataString.getBytes());
    List<OmKeyLocationInfo> locationInfos =
        groupOutputStream.getLocationInfoList();
    long containerID = locationInfos.get(0).getContainerID();
    List<DatanodeDetails> datanodes =
        cluster.getStorageContainerManager().getContainerManager()
            .getContainerWithPipeline(ContainerID.valueof(containerID))
            .getPipeline().getNodes();
    Assert.assertEquals(1, datanodes.size());
    // move the container on the datanode to Closing state, this will ensure
    // closing the key will hit BLOCK_NOT_COMMITTED_EXCEPTION while trying
    // to fetch the committed length
    for (HddsDatanodeService datanodeService : cluster.getHddsDatanodes()) {
      if (datanodes.get(0).equals(datanodeService.getDatanodeDetails())) {
        datanodeService.getDatanodeStateMachine().getContainer()
            .getContainerSet().getContainer(containerID).getContainerData()
            .setState(ContainerProtos.ContainerDataProto.State.CLOSING);
      }
    }
    dataString = fixedLengthString(keyString, (chunkSize * 1 / 2));
    key.write(dataString.getBytes());
    try {
      key.close();
      Assert.fail("Expected Exception not thrown");
    } catch (IOException ioe) {
      Assert.assertTrue(ioe instanceof StorageContainerException);
      Assert.assertTrue(((StorageContainerException) ioe).getResult()
          == ContainerProtos.Result.BLOCK_NOT_COMMITTED);
    }
    // It should retry only for max retries times
    for (int i = 1; i <= maxRetries; i++) {
      Assert.assertTrue(logCapturer.getOutput()
          .contains("Retrying GetCommittedBlockLength request"));
      Assert.assertTrue(logCapturer.getOutput().contains("Already tried " + i));
    }
    Assert.assertTrue(logCapturer.getOutput()
        .contains("GetCommittedBlockLength request failed."));
    Assert.assertTrue(logCapturer.getOutput().contains(
        "retries get failed due to exceeded maximum allowed retries number"
            + ": " + maxRetries));
    logCapturer.stopCapturing();
  }
}

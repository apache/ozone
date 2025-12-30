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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests ozone containers.
 */
public class TestOzoneContainer {
  @TempDir
  private Path tempDir;

  @Test
  public void testCreateOzoneContainer(
      @TempDir File ozoneMetaDir, @TempDir File hddsNodeDir) throws Exception {
    long containerID = ContainerTestHelper.getTestContainerID();
    OzoneConfiguration conf = newOzoneConfiguration();
    OzoneContainer container = null;
    try {
      // We don't start Ozone Container via data node, we will do it
      // independently in our test path.
      Pipeline pipeline = MockPipeline.createSingleNodePipeline();
      conf.set(OZONE_METADATA_DIRS, ozoneMetaDir.getPath());
      conf.set(HDDS_DATANODE_DIR_KEY, hddsNodeDir.getPath());
      conf.setInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
          pipeline.getFirstNode().getStandalonePort().getValue());

      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      container = ContainerTestUtils
          .getOzoneContainer(datanodeDetails, conf);
      StorageVolumeUtil.getHddsVolumesList(container.getVolumeSet().getVolumesList())
          .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempDir.toFile()));
      ContainerTestUtils.initializeDatanodeLayout(conf, datanodeDetails);
      //Set clusterId and manually start ozone container.
      container.start(UUID.randomUUID().toString());

      try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf)) {
        client.connect();
        createContainerForTesting(client, containerID);
      }
    } finally {
      if (container != null) {
        container.stop();
      }
    }
  }

  @Test
  void testOzoneContainerStart(
      @TempDir File ozoneMetaDir, @TempDir File hddsNodeDir) throws Exception {
    OzoneConfiguration conf = newOzoneConfiguration();
    OzoneContainer container = null;

    try {
      Pipeline pipeline = MockPipeline.createSingleNodePipeline();
      conf.set(OZONE_METADATA_DIRS, ozoneMetaDir.getPath());
      conf.set(HDDS_DATANODE_DIR_KEY, hddsNodeDir.getPath());
      conf.setInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
          pipeline.getFirstNode().getStandalonePort().getValue());

      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      container = ContainerTestUtils
          .getOzoneContainer(datanodeDetails, conf);
      ContainerTestUtils.initializeDatanodeLayout(conf, datanodeDetails);

      String clusterId = UUID.randomUUID().toString();
      container.start(clusterId);

      container.start(clusterId);

      container.stop();

      container.stop();

    } finally {
      if (container != null) {
        container.stop();
      }
    }
  }

  static OzoneConfiguration newOzoneConfiguration() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    return conf;
  }

  @Test
  public void testOzoneContainerViaDataNode() throws Exception {
    MiniOzoneCluster cluster = null;
    try {
      long containerID =
          ContainerTestHelper.getTestContainerID();
      OzoneConfiguration conf = newOzoneConfiguration();

      // Start ozone container Via Datanode create.
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setNumDatanodes(1)
          .build();
      cluster.waitForClusterToBeReady();

      // This client talks to ozone container via datanode.
      XceiverClientGrpc client = createClientForTesting(cluster);

      runTestOzoneContainerViaDataNode(containerID, client);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  @Flaky("HDDS-11924")
  public void testOzoneContainerWithMissingContainer() throws Exception {
    MiniOzoneCluster cluster = null;
    try {
      long containerID =
          ContainerTestHelper.getTestContainerID();
      OzoneConfiguration conf = newOzoneConfiguration();

      // Start ozone container Via Datanode create.
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setNumDatanodes(1)
          .build();
      cluster.waitForClusterToBeReady();

      runTestOzoneContainerWithMissingContainer(cluster, containerID);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void runTestOzoneContainerWithMissingContainer(
      MiniOzoneCluster cluster, long testContainerID) throws Exception {
    ContainerProtos.ContainerCommandRequestProto
        request, writeChunkRequest, putBlockRequest,
        updateRequest1, updateRequest2;
    ContainerProtos.ContainerCommandResponseProto response,
        updateResponse1, updateResponse2;
    XceiverClientGrpc client = null;
    try {
      // This client talks to ozone container via datanode.
      client = createClientForTesting(cluster);
      client.connect();
      Pipeline pipeline = client.getPipeline();
      createContainerForTesting(client, testContainerID);
      writeChunkRequest = writeChunkForContainer(client, testContainerID,
          1024);

      DatanodeDetails datanodeDetails = cluster.getHddsDatanodes().get(0).getDatanodeDetails();
      File containerPath =
          new File(cluster.getHddsDatanode(datanodeDetails).getDatanodeStateMachine()
              .getContainer().getContainerSet().getContainer(testContainerID)
              .getContainerData().getContainerPath());
      cluster.getHddsDatanode(datanodeDetails).stop();
      FileUtils.deleteDirectory(containerPath);

      // Restart & Check if the container has been marked as missing, since the container directory has been deleted.
      cluster.restartHddsDatanode(datanodeDetails, false);
      GenericTestUtils.waitFor(() -> {
        try {
          return cluster.getHddsDatanode(datanodeDetails).getDatanodeStateMachine()
              .getContainer().getContainerSet()
              .getMissingContainerSet().contains(testContainerID);
        } catch (IOException e) {
          return false;
        }
      }, 1000, 30000);

      // Read Chunk
      request = ContainerTestHelper.getReadChunkRequest(
          pipeline, writeChunkRequest.getWriteChunk());

      response = client.sendCommand(request);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.CONTAINER_NOT_FOUND, response.getResult());

      response = createContainerForTesting(client, testContainerID);
      assertEquals(ContainerProtos.Result.CONTAINER_MISSING, response.getResult());

      // Put Block
      putBlockRequest = ContainerTestHelper.getPutBlockRequest(
          pipeline, writeChunkRequest.getWriteChunk());

      response = client.sendCommand(putBlockRequest);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.CONTAINER_MISSING, response.getResult());

      // Write chunk
      response = client.sendCommand(writeChunkRequest);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.CONTAINER_MISSING, response.getResult());

      // Get Block
      request = ContainerTestHelper.
          getBlockRequest(pipeline, putBlockRequest.getPutBlock());
      response = client.sendCommand(request);
      assertEquals(ContainerProtos.Result.CONTAINER_NOT_FOUND, response.getResult());

      // Create Container
      request  = ContainerTestHelper.getCreateContainerRequest(testContainerID, pipeline);
      response = client.sendCommand(request);
      assertEquals(ContainerProtos.Result.CONTAINER_MISSING, response.getResult());

      // Delete Block and Delete Chunk are handled by BlockDeletingService
      // ContainerCommandRequestProto DeleteBlock and DeleteChunk requests
      // are deprecated

      //Update an existing container
      Map<String, String> containerUpdate = new HashMap<String, String>();
      containerUpdate.put("container_updated_key", "container_updated_value");
      updateRequest1 = ContainerTestHelper.getUpdateContainerRequest(
          testContainerID, containerUpdate);
      updateResponse1 = client.sendCommand(updateRequest1);
      assertNotNull(updateResponse1);
      assertEquals(ContainerProtos.Result.CONTAINER_MISSING, updateResponse1.getResult());

      //Update an non-existing container
      long nonExistingContinerID =
          ContainerTestHelper.getTestContainerID();
      updateRequest2 = ContainerTestHelper.getUpdateContainerRequest(
          nonExistingContinerID, containerUpdate);
      updateResponse2 = client.sendCommand(updateRequest2);
      assertEquals(ContainerProtos.Result.CONTAINER_NOT_FOUND,
          updateResponse2.getResult());

      // Restarting again & checking if the container is still not present on disk and marked as missing, this is to
      // ensure the previous write request didn't inadvertently create the container data.
      cluster.restartHddsDatanode(datanodeDetails, false);
      GenericTestUtils.waitFor(() -> {
        try {
          return cluster.getHddsDatanode(datanodeDetails).getDatanodeStateMachine()
              .getContainer().getContainerSet()
              .getMissingContainerSet().contains(testContainerID);
        } catch (IOException e) {
          return false;
        }
      }, 1000, 30000);
      // Create Recovering Container
      request  = ContainerTestHelper.getCreateContainerRequest(testContainerID, pipeline,
          ContainerProtos.ContainerDataProto.State.RECOVERING);
      response = client.sendCommand(request);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      //write chunk on recovering container
      response = client.sendCommand(writeChunkRequest);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      //write chunk on recovering container
      response = client.sendCommand(putBlockRequest);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      //Get block on the recovering container should succeed now.
      request = ContainerTestHelper.getBlockRequest(pipeline, putBlockRequest.getPutBlock());
      response = client.sendCommand(request);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  public static void runTestOzoneContainerViaDataNode(
      long testContainerID, XceiverClientSpi client) throws Exception {
    ContainerProtos.ContainerCommandRequestProto
        request, writeChunkRequest, putBlockRequest,
        updateRequest1, updateRequest2;
    ContainerProtos.ContainerCommandResponseProto response,
        updateResponse1, updateResponse2;
    try {
      client.connect();

      Pipeline pipeline = client.getPipeline();
      createContainerForTesting(client, testContainerID);
      writeChunkRequest = writeChunkForContainer(client, testContainerID,
          1024);

      // Read Chunk
      request = ContainerTestHelper.getReadChunkRequest(
          pipeline, writeChunkRequest.getWriteChunk());

      response = client.sendCommand(request);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

      // Put Block
      putBlockRequest = ContainerTestHelper.getPutBlockRequest(
          pipeline, writeChunkRequest.getWriteChunk());


      response = client.sendCommand(putBlockRequest);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

      // Get Block
      request = ContainerTestHelper.
          getBlockRequest(pipeline, putBlockRequest.getPutBlock());
      response = client.sendCommand(request);
      int chunksCount = putBlockRequest.getPutBlock().getBlockData().
          getChunksCount();
      ContainerTestHelper.verifyGetBlock(response, chunksCount);

      // Delete Block and Delete Chunk are handled by BlockDeletingService
      // ContainerCommandRequestProto DeleteBlock and DeleteChunk requests
      // are deprecated

      //Update an existing container
      Map<String, String> containerUpdate = new HashMap<String, String>();
      containerUpdate.put("container_updated_key", "container_updated_value");
      updateRequest1 = ContainerTestHelper.getUpdateContainerRequest(
          testContainerID, containerUpdate);
      updateResponse1 = client.sendCommand(updateRequest1);
      assertNotNull(updateResponse1);
      assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      //Update an non-existing container
      long nonExistingContinerID =
          ContainerTestHelper.getTestContainerID();
      updateRequest2 = ContainerTestHelper.getUpdateContainerRequest(
          nonExistingContinerID, containerUpdate);
      updateResponse2 = client.sendCommand(updateRequest2);
      assertEquals(ContainerProtos.Result.CONTAINER_NOT_FOUND,
          updateResponse2.getResult());
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testBothGetandPutSmallFile(
      @TempDir File ozoneMetaDir, @TempDir File hddsNodeDir) throws Exception {
    MiniOzoneCluster cluster = null;
    XceiverClientGrpc client = null;
    try {
      OzoneConfiguration conf = newOzoneConfiguration();
      conf.set(OZONE_METADATA_DIRS, ozoneMetaDir.getPath());
      conf.set(HDDS_DATANODE_DIR_KEY, hddsNodeDir.getPath());
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setNumDatanodes(1)
          .build();
      cluster.waitForClusterToBeReady();
      long containerID = ContainerTestHelper.getTestContainerID();

      client = createClientForTesting(cluster);
      runTestBothGetandPutSmallFile(containerID, client);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  static void runTestBothGetandPutSmallFile(
      long containerID, XceiverClientSpi client) throws Exception {
    try {
      client.connect();

      createContainerForTesting(client, containerID);

      BlockID blockId = ContainerTestHelper.getTestBlockID(containerID);
      final ContainerProtos.ContainerCommandRequestProto smallFileRequest
          = ContainerTestHelper.getWriteSmallFileRequest(
          client.getPipeline(), blockId, 1024);
      final byte[] requestBytes = smallFileRequest.getPutSmallFile().getData()
          .toByteArray();
      ContainerProtos.ContainerCommandResponseProto response
          = client.sendCommand(smallFileRequest);
      assertNotNull(response);

      final ContainerProtos.ContainerCommandRequestProto getSmallFileRequest
          = ContainerTestHelper.getReadSmallFileRequest(client.getPipeline(),
          smallFileRequest.getPutSmallFile().getBlock());
      response = client.sendCommand(getSmallFileRequest);

      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

      ContainerProtos.ReadChunkResponseProto chunkResponse =
          response.getGetSmallFile().getData();
      if (chunkResponse.hasDataBuffers()) {
        assertArrayEquals(requestBytes,
            chunkResponse.getDataBuffers().toByteArray());
      } else {
        assertArrayEquals(requestBytes,
            chunkResponse.getData().toByteArray());
      }
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testCloseContainer(
      @TempDir File ozoneMetaDir, @TempDir File hddsNodeDir) throws Exception {
    MiniOzoneCluster cluster = null;
    XceiverClientGrpc client = null;
    ContainerProtos.ContainerCommandResponseProto response;
    ContainerProtos.ContainerCommandRequestProto
        writeChunkRequest, putBlockRequest, request;
    try {

      OzoneConfiguration conf = newOzoneConfiguration();
      conf.set(OZONE_METADATA_DIRS, ozoneMetaDir.getPath());
      conf.set(HDDS_DATANODE_DIR_KEY, hddsNodeDir.getPath());
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setNumDatanodes(1)
          .build();
      cluster.waitForClusterToBeReady();

      client = createClientForTesting(cluster);
      client.connect();

      long containerID = ContainerTestHelper.getTestContainerID();
      createContainerForTesting(client, containerID);
      writeChunkRequest = writeChunkForContainer(client, containerID,
          1024);


      putBlockRequest = ContainerTestHelper.getPutBlockRequest(
          client.getPipeline(), writeChunkRequest.getWriteChunk());
      // Put block before closing.
      response = client.sendCommand(putBlockRequest);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      // Close the container.
      request = ContainerTestHelper.getCloseContainer(
          client.getPipeline(), containerID);
      response = client.sendCommand(request);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());


      // Assert that none of the write  operations are working after close.

      // Write chunks should fail now.

      response = client.sendCommand(writeChunkRequest);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.CLOSED_CONTAINER_IO,
          response.getResult());

      // Read chunk must work on a closed container.
      request = ContainerTestHelper.getReadChunkRequest(client.getPipeline(),
          writeChunkRequest.getWriteChunk());
      response = client.sendCommand(request);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

      // Put block will fail on a closed container.
      response = client.sendCommand(putBlockRequest);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.CLOSED_CONTAINER_IO,
          response.getResult());

      // Get block must work on the closed container.
      request = ContainerTestHelper.getBlockRequest(client.getPipeline(),
          putBlockRequest.getPutBlock());
      response = client.sendCommand(request);
      int chunksCount = putBlockRequest.getPutBlock().getBlockData()
          .getChunksCount();
      ContainerTestHelper.verifyGetBlock(response, chunksCount);
    } finally {
      if (client != null) {
        client.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeleteContainer(
      @TempDir File ozoneMetaDir, @TempDir File hddsNodeDir) throws Exception {
    MiniOzoneCluster cluster = null;
    XceiverClientGrpc client = null;
    ContainerProtos.ContainerCommandResponseProto response;
    ContainerProtos.ContainerCommandRequestProto request,
        writeChunkRequest, putBlockRequest;
    try {
      OzoneConfiguration conf = newOzoneConfiguration();
      conf.set(OZONE_METADATA_DIRS, ozoneMetaDir.getPath());
      conf.set(HDDS_DATANODE_DIR_KEY, hddsNodeDir.getPath());
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setNumDatanodes(1)
          .build();
      cluster.waitForClusterToBeReady();

      client = createClientForTesting(cluster);
      client.connect();

      long containerID = ContainerTestHelper.getTestContainerID();
      createContainerForTesting(client, containerID);
      writeChunkRequest = writeChunkForContainer(
          client, containerID, 1024);

      putBlockRequest = ContainerTestHelper.getPutBlockRequest(
          client.getPipeline(), writeChunkRequest.getWriteChunk());
      // Put key before deleting.
      response = client.sendCommand(putBlockRequest);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      // Container cannot be deleted because force flag is set to false and
      // the container is still open
      request = ContainerTestHelper.getDeleteContainer(
          client.getPipeline(), containerID, false);
      response = client.sendCommand(request);

      assertNotNull(response);
      assertEquals(ContainerProtos.Result.DELETE_ON_OPEN_CONTAINER,
          response.getResult());

      // Container can be deleted, by setting force flag, even with out closing
      request = ContainerTestHelper.getDeleteContainer(
          client.getPipeline(), containerID, true);
      response = client.sendCommand(request);

      assertNotNull(response);
      assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

    } finally {
      if (client != null) {
        client.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  // Runs a set of commands as Async calls and verifies that calls indeed worked
  // as expected.
  static void runAsyncTests(
      long containerID, XceiverClientSpi client) throws Exception {
    try {
      client.connect();

      createContainerForTesting(client, containerID);
      final List<CompletableFuture> computeResults = new LinkedList<>();
      int requestCount = 1000;
      // Create a bunch of Async calls from this test.
      for (int x = 0; x < requestCount; x++) {
        BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
        final ContainerProtos.ContainerCommandRequestProto smallFileRequest
            = ContainerTestHelper.getWriteSmallFileRequest(
            client.getPipeline(), blockID, 1024);

        CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
            response = client.sendCommandAsync(smallFileRequest).getResponse();
        computeResults.add(response);
      }

      CompletableFuture<Void> combinedFuture =
          CompletableFuture.allOf(computeResults.toArray(
              new CompletableFuture[computeResults.size()]));
      // Wait for all futures to complete.
      combinedFuture.get();
      // Assert that all futures are indeed done.
      for (CompletableFuture future : computeResults) {
        assertTrue(future.isDone());
      }
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testXcieverClientAsync(
      @TempDir File ozoneMetaDir, @TempDir File hddsNodeDir) throws Exception {
    MiniOzoneCluster cluster = null;
    XceiverClientGrpc client = null;
    try {
      OzoneConfiguration conf = newOzoneConfiguration();
      conf.set(OZONE_METADATA_DIRS, ozoneMetaDir.getPath());
      conf.set(HDDS_DATANODE_DIR_KEY, hddsNodeDir.getPath());
      cluster = MiniOzoneCluster.newBuilder(conf)
          .setNumDatanodes(1)
          .build();
      cluster.waitForClusterToBeReady();
      long containerID = ContainerTestHelper.getTestContainerID();

      client = createClientForTesting(cluster);
      runAsyncTests(containerID, client);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static XceiverClientGrpc createClientForTesting(
      MiniOzoneCluster cluster) {
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipelines().iterator().next();
    return createClientForTesting(pipeline, cluster);
  }

  private static XceiverClientGrpc createClientForTesting(Pipeline pipeline, MiniOzoneCluster cluster) {
    return new XceiverClientGrpc(pipeline, cluster.getConf());
  }

  public static ContainerProtos.ContainerCommandResponseProto createContainerForTesting(XceiverClientSpi client,
      long containerID) throws Exception {
    // Create container
    ContainerProtos.ContainerCommandRequestProto request =
        ContainerTestHelper.getCreateContainerRequest(
            containerID, client.getPipeline());
    ContainerProtos.ContainerCommandResponseProto response =
        client.sendCommand(request);
    assertNotNull(response);
    return response;
  }

  public static ContainerProtos.ContainerCommandRequestProto
      writeChunkForContainer(XceiverClientSpi client,
      long containerID, int dataLen) throws Exception {
    // Write Chunk
    BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerTestHelper.getWriteChunkRequest(client.getPipeline(),
            blockID, dataLen);
    ContainerProtos.ContainerCommandResponseProto response =
        client.sendCommand(writeChunkRequest);
    assertNotNull(response);
    assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    return writeChunkRequest;
  }
}

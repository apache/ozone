/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.upgrade;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.ScmTestMock;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests upgrading a single datanode from HADOOP_PRC_PORTS_IN_DATANODEDETAILS to HBASE_SUPPORT.
 */
public class TestDatanodeUpgradeToHBaseSupport {
  @TempDir
  private Path tempFolder;

  private DatanodeStateMachine dsm;
  private OzoneConfiguration conf;
  private static final String CLUSTER_ID = "clusterID";

  private RPC.Server scmRpcServer;
  private InetSocketAddress address;

  private Random random;

  private void initTests() throws Exception {
    conf = new OzoneConfiguration();
    setup();
  }

  private void setup() throws Exception {
    random = new Random();

    address = SCMTestUtils.getReuseableAddress();
    conf.setSocketAddr(ScmConfigKeys.OZONE_SCM_NAMES, address);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempFolder.toString());
  }

  @AfterEach
  public void teardown() throws Exception {
    if (scmRpcServer != null) {
      scmRpcServer.stop();
    }

    if (dsm != null) {
      dsm.close();
    }
  }

  /**
   * Test incremental chunk list before and after finalization.
   */
  @Test
  public void testIncrementalChunkListBeforeAndAfterUpgrade() throws Exception {
    initTests();
    // start DN and SCM
    startScmServer();
    addHddsVolume();
    startPreFinalizedDatanode();
    final Pipeline pipeline = getPipeline();

    // Add data to read.
    final long containerID = addContainer(pipeline);
    // incremental chunk list should be rejected before finalizing.
    putBlock(containerID, pipeline, true, ContainerProtos.Result.UNSUPPORTED_REQUEST);
    Container<?> container = dsm.getContainer().getContainerSet().getContainer(containerID);
    assertEquals(OPEN, container.getContainerData().getState());
    // close container to allow upgrade.
    closeContainer(containerID, pipeline);

    dsm.finalizeUpgrade();
    assertTrue(dsm.getLayoutVersionManager().isAllowed(HDDSLayoutFeature.HBASE_SUPPORT));
    // open a new container after finalization
    final long containerID2 = addContainer(pipeline);
    // incremental chunk list should work after finalizing.
    putBlock(containerID2, pipeline, true);
    Container<?> container2 = dsm.getContainer().getContainerSet().getContainer(containerID2);
    assertEquals(OPEN, container2.getContainerData().getState());
  }

  /**
   * Test block finalization before and after upgrade finalization.
   */
  @Test
  public void testBlockFinalizationBeforeAndAfterUpgrade() throws Exception {
    initTests();
    // start DN and SCM
    startScmServer();
    addHddsVolume();
    startPreFinalizedDatanode();
    final Pipeline pipeline = getPipeline();

    // Add data to read.
    final long containerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk = putBlock(containerID, pipeline, false);
    finalizeBlock(containerID, writeChunk.getBlockID().getLocalID(), ContainerProtos.Result.UNSUPPORTED_REQUEST);
    Container<?> container = dsm.getContainer().getContainerSet().getContainer(containerID);
    assertEquals(OPEN, container.getContainerData().getState());
    // close container to allow upgrade.
    closeContainer(containerID, pipeline);

    dsm.finalizeUpgrade();
    assertTrue(dsm.getLayoutVersionManager().isAllowed(HDDSLayoutFeature.HBASE_SUPPORT));
    final long containerID2 = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk2 = putBlock(containerID2, pipeline, false);
    // Make sure we can read after finalizing too.
    finalizeBlock(containerID2, writeChunk2.getBlockID().getLocalID(), ContainerProtos.Result.SUCCESS);
    Container<?> container2 = dsm.getContainer().getContainerSet().getContainer(containerID2);
    assertEquals(OPEN, container2.getContainerData().getState());
  }

  /**
   * Starts the datanode with the fore layout version, and calls the version
   * endpoint task to get cluster ID and SCM ID.
   *
   * The daemon for the datanode state machine is not started in this test.
   * This greatly speeds up execution time.
   * It means we do not have heartbeat functionality or pre-finalize
   * upgrade actions, but neither of those things are needed for these tests.
   */
  public void startPreFinalizedDatanode() throws Exception {
    // Set layout version.
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempFolder.toString());
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.HADOOP_PRC_PORTS_IN_DATANODEDETAILS.layoutVersion());
    layoutStorage.initialize();

    // Build and start the datanode.
    DatanodeDetails dd = ContainerTestUtils.createDatanodeDetails();
    DatanodeStateMachine newDsm = new DatanodeStateMachine(dd, conf);
    int actualMlv = newDsm.getLayoutVersionManager().getMetadataLayoutVersion();
    assertEquals(
        HDDSLayoutFeature.HADOOP_PRC_PORTS_IN_DATANODEDETAILS.layoutVersion(),
        actualMlv);
    if (dsm != null) {
      dsm.close();
    }
    dsm = newDsm;

    callVersionEndpointTask();
  }

  /**
   * Get the cluster ID and SCM ID from SCM to the datanode.
   */
  public void callVersionEndpointTask() throws Exception {
    try (EndpointStateMachine esm = ContainerTestUtils.createEndpoint(conf,
        address, 1000)) {
      VersionEndpointTask vet = new VersionEndpointTask(esm, conf,
          dsm.getContainer());
      esm.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      vet.call();
    }
  }

  public String startScmServer() throws IOException {
    String scmID = UUID.randomUUID().toString();
    ScmTestMock scmServerImpl = new ScmTestMock(CLUSTER_ID, scmID);
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        scmServerImpl, address, 10);
    return scmID;
  }

  /// CONTAINER OPERATIONS ///
  public void readChunk(ContainerProtos.WriteChunkRequestProto writeChunk,
      Pipeline pipeline)  throws Exception {
    ContainerProtos.ContainerCommandRequestProto readChunkRequest =
        ContainerTestHelper.getReadChunkRequest(pipeline, writeChunk);

    dispatchRequest(readChunkRequest);
  }

  public ContainerProtos.WriteChunkRequestProto putBlock(long containerID,
      Pipeline pipeline, boolean incremental) throws Exception {
    return putBlock(containerID, pipeline, incremental, ContainerProtos.Result.SUCCESS);
  }

  public ContainerProtos.WriteChunkRequestProto putBlock(long containerID,
      Pipeline pipeline, boolean incremental, ContainerProtos.Result expectedResult) throws Exception {
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        getWriteChunk(containerID, pipeline);
    dispatchRequest(writeChunkRequest);

    ContainerProtos.ContainerCommandRequestProto putBlockRequest =
        ContainerTestHelper.getPutBlockRequest(pipeline,
            writeChunkRequest.getWriteChunk(), incremental);
    dispatchRequest(putBlockRequest, expectedResult);

    return writeChunkRequest.getWriteChunk();
  }

  public ContainerProtos.ContainerCommandRequestProto getWriteChunk(
      long containerID, Pipeline pipeline) throws Exception {
    return ContainerTestHelper.getWriteChunkRequest(pipeline,
            ContainerTestHelper.getTestBlockID(containerID), 100);
  }

  public Pipeline getPipeline() {
    return MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));
  }

  public long addContainer(Pipeline pipeline)
      throws Exception {
    long containerID = random.nextInt(Integer.MAX_VALUE);
    ContainerProtos.ContainerCommandRequestProto createContainerRequest =
        ContainerTestHelper.getCreateContainerRequest(containerID, pipeline);
    dispatchRequest(createContainerRequest);

    return containerID;
  }

  public void deleteContainer(long containerID, Pipeline pipeline)
      throws Exception {
    ContainerProtos.ContainerCommandRequestProto deleteContainerRequest =
        ContainerTestHelper.getDeleteContainer(pipeline, containerID, true);
    dispatchRequest(deleteContainerRequest);
  }

  public void closeContainer(long containerID, Pipeline pipeline)
      throws Exception {
    closeContainer(containerID, pipeline, ContainerProtos.Result.SUCCESS);
  }

  public void closeContainer(long containerID, Pipeline pipeline,
      ContainerProtos.Result expectedResult) throws Exception {
    ContainerProtos.ContainerCommandRequestProto closeContainerRequest =
        ContainerTestHelper.getCloseContainer(pipeline, containerID);
    dispatchRequest(closeContainerRequest, expectedResult);
  }

  public void finalizeBlock(long containerID, long localID, ContainerProtos.Result expectedResult) {
    ContainerInfo container = mock(ContainerInfo.class);
    when(container.getContainerID()).thenReturn(containerID);

    ContainerProtos.ContainerCommandRequestProto finalizeBlockRequest =
        ContainerTestHelper.getFinalizeBlockRequest(localID, container, UUID.randomUUID().toString());

    dispatchRequest(finalizeBlockRequest, expectedResult);
  }

  public void dispatchRequest(
      ContainerProtos.ContainerCommandRequestProto request) {
    dispatchRequest(request, ContainerProtos.Result.SUCCESS);
  }

  public void dispatchRequest(
      ContainerProtos.ContainerCommandRequestProto request,
      ContainerProtos.Result expectedResult) {
    ContainerProtos.ContainerCommandResponseProto response =
        dsm.getContainer().getDispatcher().dispatch(request, null);
    assertEquals(expectedResult, response.getResult());
  }

  /// VOLUME OPERATIONS ///

  /**
   * Append a datanode volume to the existing volumes in the configuration.
   * @return The root directory for the new volume.
   */
  public File addHddsVolume() throws IOException {

    File vol = Files.createDirectory(tempFolder.resolve(UUID.randomUUID()
        .toString())).toFile();
    String[] existingVolumes =
        conf.getStrings(ScmConfigKeys.HDDS_DATANODE_DIR_KEY);
    List<String> allVolumes = new ArrayList<>();
    if (existingVolumes != null) {
      allVolumes.addAll(Arrays.asList(existingVolumes));
    }

    allVolumes.add(vol.getAbsolutePath());
    conf.setStrings(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        allVolumes.toArray(new String[0]));

    return vol;
  }
}

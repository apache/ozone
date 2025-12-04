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

package org.apache.hadoop.ozone.container.upgrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;

/**
 * Helpers for upgrade tests.
 */
public final class UpgradeTestHelper {
  private static final Random RANDOM = new Random();

  private UpgradeTestHelper() {
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
  public static DatanodeStateMachine startPreFinalizedDatanode(
      OzoneConfiguration conf, Path tempFolder,
      DatanodeStateMachine dsm, InetSocketAddress address,
      int metadataLayoutVersion)
      throws Exception {
    // Set layout version.
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempFolder.toString());
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        metadataLayoutVersion);
    layoutStorage.initialize();
    if (dsm != null) {
      dsm.close();
    }

    // Build and start the datanode.
    DatanodeDetails dd = ContainerTestUtils.createDatanodeDetails();
    dsm = new DatanodeStateMachine(dd, conf);
    int actualMlv = dsm.getLayoutVersionManager().getMetadataLayoutVersion();
    assertEquals(
        metadataLayoutVersion,
        actualMlv);


    callVersionEndpointTask(conf, dsm.getContainer(), address);
    return dsm;
  }

  public static DatanodeStateMachine restartDatanode(
      OzoneConfiguration conf, DatanodeStateMachine dsm, boolean shouldSetDbParentDir,
      Path tempFolder, InetSocketAddress address, int expectedMlv, boolean exactMatch)
      throws Exception {
    // Stop existing datanode.
    DatanodeDetails dd = dsm.getDatanodeDetails();
    dsm.close();

    // Start new datanode with the same configuration.
    dsm = new DatanodeStateMachine(dd, conf);
    if (shouldSetDbParentDir) {
      StorageVolumeUtil.getHddsVolumesList(dsm.getContainer().getVolumeSet().getVolumesList())
          .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempFolder.toFile()));
    }
    int mlv = dsm.getLayoutVersionManager().getMetadataLayoutVersion();
    if (exactMatch) {
      assertEquals(expectedMlv, mlv);
    } else {
      assertThat(expectedMlv).isLessThanOrEqualTo(mlv);
    }

    callVersionEndpointTask(conf, dsm.getContainer(), address);
    return dsm;
  }

  /**
   * Get the cluster ID and SCM ID from SCM to the datanode.
   */
  public static void callVersionEndpointTask(
      OzoneConfiguration conf, OzoneContainer container, InetSocketAddress address)
      throws Exception {
    try (EndpointStateMachine esm = ContainerTestUtils.createEndpoint(conf,
        address, 1000)) {
      VersionEndpointTask vet = new VersionEndpointTask(esm, conf,
          container);
      esm.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      vet.call();
    }
  }

  /**
   * Append a datanode volume to the existing volumes in the configuration.
   * @return The root directory for the new volume.
   */
  public static File addHddsVolume(OzoneConfiguration conf, Path tempFolder) throws IOException {

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

  /**
   * Append a db volume to the existing volumes in the configuration.
   * @return The root directory for the new volume.
   */
  public static File addDbVolume(OzoneConfiguration conf, Path tempFolder) throws Exception {
    File vol = Files.createDirectory(tempFolder.resolve(UUID.randomUUID()
        .toString())).toFile();
    String[] existingVolumes =
        conf.getStrings(OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR);
    List<String> allVolumes = new ArrayList<>();
    if (existingVolumes != null) {
      allVolumes.addAll(Arrays.asList(existingVolumes));
    }

    allVolumes.add(vol.getAbsolutePath());
    conf.setStrings(OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR,
        allVolumes.toArray(new String[0]));

    return vol;
  }

  public static void dispatchRequest(
      ContainerDispatcher dispatcher,
      ContainerProtos.ContainerCommandRequestProto request) {
    dispatchRequest(dispatcher, request, ContainerProtos.Result.SUCCESS);
  }

  public static void dispatchRequest(
      ContainerDispatcher dispatcher, ContainerProtos.ContainerCommandRequestProto request,
      ContainerProtos.Result expectedResult) {
    ContainerProtos.ContainerCommandResponseProto response =
        dispatcher.dispatch(request, null);
    assertEquals(expectedResult, response.getResult());
  }

  public static void readChunk(
      ContainerDispatcher dispatcher, ContainerProtos.WriteChunkRequestProto writeChunk,
      Pipeline pipeline) throws Exception {
    ContainerProtos.ContainerCommandRequestProto readChunkRequest =
        ContainerTestHelper.getReadChunkRequest(pipeline, writeChunk);
    dispatchRequest(dispatcher, readChunkRequest);
  }

  public static ContainerProtos.WriteChunkRequestProto putBlock(
      ContainerDispatcher dispatcher, long containerID, Pipeline pipeline,
      boolean incremental) throws Exception {
    return putBlock(dispatcher, containerID, pipeline, incremental, ContainerProtos.Result.SUCCESS);
  }

  public static ContainerProtos.WriteChunkRequestProto putBlock(
      ContainerDispatcher dispatcher, long containerID, Pipeline pipeline) throws Exception {
    return putBlock(dispatcher, containerID, pipeline, false, ContainerProtos.Result.SUCCESS);
  }

  public static ContainerProtos.WriteChunkRequestProto putBlock(
      ContainerDispatcher dispatcher, long containerID, Pipeline pipeline,
      boolean incremental, ContainerProtos.Result expectedResult) throws Exception {
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerTestHelper.getWriteChunkRequest(pipeline,
            ContainerTestHelper.getTestBlockID(containerID), 100);
    dispatchRequest(dispatcher, writeChunkRequest);

    ContainerProtos.ContainerCommandRequestProto putBlockRequest =
        ContainerTestHelper.getPutBlockRequest(pipeline,
            writeChunkRequest.getWriteChunk(), incremental);
    dispatchRequest(dispatcher, putBlockRequest, expectedResult);
    return writeChunkRequest.getWriteChunk();
  }

  public static long addContainer(ContainerDispatcher dispatcher, Pipeline pipeline)
      throws Exception {
    long containerID = RANDOM.nextInt(Integer.MAX_VALUE);
    ContainerProtos.ContainerCommandRequestProto createContainerRequest =
        ContainerTestHelper.getCreateContainerRequest(containerID, pipeline);
    dispatchRequest(dispatcher, createContainerRequest);

    return containerID;
  }

  public static void deleteContainer(ContainerDispatcher dispatcher, long containerID, Pipeline pipeline)
      throws Exception {
    ContainerProtos.ContainerCommandRequestProto deleteContainerRequest =
        ContainerTestHelper.getDeleteContainer(pipeline, containerID, true);
    dispatchRequest(dispatcher, deleteContainerRequest);
  }

  public static void closeContainer(ContainerDispatcher dispatcher, long containerID, Pipeline pipeline)
      throws Exception {
    closeContainer(dispatcher, containerID, pipeline, ContainerProtos.Result.SUCCESS);
  }

  public static void closeContainer(
      ContainerDispatcher dispatcher, long containerID, Pipeline pipeline,
      ContainerProtos.Result expectedResult) throws Exception {
    ContainerProtos.ContainerCommandRequestProto closeContainerRequest =
        ContainerTestHelper.getCloseContainer(pipeline, containerID);
    dispatchRequest(dispatcher, closeContainerRequest, expectedResult);
  }

  public static void finalizeBlock(
      ContainerDispatcher dispatcher, long containerID, long localID, ContainerProtos.Result expectedResult) {
    ContainerInfo container = mock(ContainerInfo.class);
    when(container.getContainerID()).thenReturn(containerID);

    ContainerProtos.ContainerCommandRequestProto finalizeBlockRequest =
        ContainerTestHelper.getFinalizeBlockRequest(localID, container, UUID.randomUUID().toString());

    UpgradeTestHelper.dispatchRequest(dispatcher, finalizeBlockRequest, expectedResult);
  }
}

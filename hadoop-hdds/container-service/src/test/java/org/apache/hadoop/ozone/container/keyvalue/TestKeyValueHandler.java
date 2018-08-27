/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import org.mockito.Mockito;

import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_DATANODE_VOLUME_CHOOSING_POLICY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.times;


import java.io.File;
import java.util.UUID;

/**
 * Unit tests for {@link KeyValueHandler}.
 */
public class TestKeyValueHandler {

  @Rule
  public TestRule timeout = new Timeout(300000);

  private static HddsDispatcher dispatcher;
  private static KeyValueHandler handler;

  private final static String DATANODE_UUID = UUID.randomUUID().toString();

  private final String baseDir = MiniDFSCluster.getBaseDirectory();
  private final String volume = baseDir + "disk1";

  private static final long DUMMY_CONTAINER_ID = 9999;

  @BeforeClass
  public static void setup() throws StorageContainerException {
    // Create mock HddsDispatcher and KeyValueHandler.
    handler = Mockito.mock(KeyValueHandler.class);
    dispatcher = Mockito.mock(HddsDispatcher.class);
    Mockito.when(dispatcher.getHandler(any())).thenReturn(handler);
    Mockito.when(dispatcher.dispatch(any())).thenCallRealMethod();
    Mockito.when(dispatcher.getContainer(anyLong())).thenReturn(
        Mockito.mock(KeyValueContainer.class));
    Mockito.when(handler.handle(any(), any())).thenCallRealMethod();
    doCallRealMethod().when(dispatcher).setMetricsForTesting(any());
    dispatcher.setMetricsForTesting(Mockito.mock(ContainerMetrics.class));
  }

  @Test
  /**
   * Test that Handler handles different command types correctly.
   */
  public void testHandlerCommandHandling() throws Exception {

    // Test Create Container Request handling
    ContainerCommandRequestProto createContainerRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setContainerID(DUMMY_CONTAINER_ID)
            .setDatanodeUuid(DATANODE_UUID)
            .setCreateContainer(ContainerProtos.CreateContainerRequestProto
                .getDefaultInstance())
            .build();
    dispatcher.dispatch(createContainerRequest);
    Mockito.verify(handler, times(1)).handleCreateContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Read Container Request handling
    ContainerCommandRequestProto readContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ReadContainer);
    dispatcher.dispatch(readContainerRequest);
    Mockito.verify(handler, times(1)).handleReadContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Update Container Request handling
    ContainerCommandRequestProto updateContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.UpdateContainer);
    dispatcher.dispatch(updateContainerRequest);
    Mockito.verify(handler, times(1)).handleUpdateContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Delete Container Request handling
    ContainerCommandRequestProto deleteContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.DeleteContainer);
    dispatcher.dispatch(deleteContainerRequest);
    Mockito.verify(handler, times(1)).handleDeleteContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test List Container Request handling
    ContainerCommandRequestProto listContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListContainer);
    dispatcher.dispatch(listContainerRequest);
    Mockito.verify(handler, times(1)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Close Container Request handling
    ContainerCommandRequestProto closeContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.CloseContainer);
    dispatcher.dispatch(closeContainerRequest);
    Mockito.verify(handler, times(1)).handleCloseContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Put Key Request handling
    ContainerCommandRequestProto putKeyRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.PutKey);
    dispatcher.dispatch(putKeyRequest);
    Mockito.verify(handler, times(1)).handlePutKey(
        any(ContainerCommandRequestProto.class), any());

    // Test Get Key Request handling
    ContainerCommandRequestProto getKeyRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.GetKey);
    dispatcher.dispatch(getKeyRequest);
    Mockito.verify(handler, times(1)).handleGetKey(
        any(ContainerCommandRequestProto.class), any());

    // Test Delete Key Request handling
    ContainerCommandRequestProto deleteKeyRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.DeleteKey);
    dispatcher.dispatch(deleteKeyRequest);
    Mockito.verify(handler, times(1)).handleDeleteKey(
        any(ContainerCommandRequestProto.class), any());

    // Test List Key Request handling
    ContainerCommandRequestProto listKeyRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListKey);
    dispatcher.dispatch(listKeyRequest);
    Mockito.verify(handler, times(2)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Read Chunk Request handling
    ContainerCommandRequestProto readChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ReadChunk);
    dispatcher.dispatch(readChunkRequest);
    Mockito.verify(handler, times(1)).handleReadChunk(
        any(ContainerCommandRequestProto.class), any());

    // Test Delete Chunk Request handling
    ContainerCommandRequestProto deleteChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.DeleteChunk);
    dispatcher.dispatch(deleteChunkRequest);
    Mockito.verify(handler, times(1)).handleDeleteChunk(
        any(ContainerCommandRequestProto.class), any());

    // Test Write Chunk Request handling
    ContainerCommandRequestProto writeChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.WriteChunk);
    dispatcher.dispatch(writeChunkRequest);
    Mockito.verify(handler, times(1)).handleWriteChunk(
        any(ContainerCommandRequestProto.class), any());

    // Test List Chunk Request handling
    ContainerCommandRequestProto listChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListChunk);
    dispatcher.dispatch(listChunkRequest);
    Mockito.verify(handler, times(3)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Put Small File Request handling
    ContainerCommandRequestProto putSmallFileRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.PutSmallFile);
    dispatcher.dispatch(putSmallFileRequest);
    Mockito.verify(handler, times(1)).handlePutSmallFile(
        any(ContainerCommandRequestProto.class), any());

    // Test Get Small File Request handling
    ContainerCommandRequestProto getSmallFileRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.GetSmallFile);
    dispatcher.dispatch(getSmallFileRequest);
    Mockito.verify(handler, times(1)).handleGetSmallFile(
        any(ContainerCommandRequestProto.class), any());
  }

  @Test
  public void testVolumeSetInKeyValueHandler() throws Exception{
    File path = GenericTestUtils.getRandomizedTestDir();
    try {
      Configuration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, path.getAbsolutePath());
      ContainerSet cset = new ContainerSet();
      int[] interval = new int[1];
      interval[0] = 2;
      ContainerMetrics metrics = new ContainerMetrics(interval);
      VolumeSet volumeSet = new VolumeSet(UUID.randomUUID().toString(), conf);
      KeyValueHandler keyValueHandler = new KeyValueHandler(conf, cset,
          volumeSet, metrics);
      assertEquals(keyValueHandler.getVolumeChoosingPolicyForTesting()
          .getClass().getName(), "org.apache.hadoop.ozone.container.common" +
          ".volume.RoundRobinVolumeChoosingPolicy");

      //Set a class which is not of sub class of VolumeChoosingPolicy
      conf.set(HDDS_DATANODE_VOLUME_CHOOSING_POLICY,
          "org.apache.hadoop.ozone.container.common.impl.HddsDispatcher");
      try {
        new KeyValueHandler(conf, cset, volumeSet, metrics);
      } catch (RuntimeException ex) {
        GenericTestUtils.assertExceptionContains("class org.apache.hadoop" +
            ".ozone.container.common.impl.HddsDispatcher not org.apache" +
            ".hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy",
            ex);
      }
    } finally {
      FileUtil.fullyDelete(path);
    }
  }

  private ContainerCommandRequestProto getDummyCommandRequestProto(
      ContainerProtos.Type cmdType) {
    ContainerCommandRequestProto request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(cmdType)
            .setContainerID(DUMMY_CONTAINER_ID)
            .setDatanodeUuid(DATANODE_UUID)
            .build();

    return request;
  }


  @Test
  public void testCloseInvalidContainer() {
    long containerID = 1234L;
    Configuration conf = new Configuration();
    KeyValueContainerData kvData = new KeyValueContainerData(containerID,
        (long) StorageUnit.GB.toBytes(1));
    KeyValueContainer container = new KeyValueContainer(kvData, conf);
    kvData.setState(ContainerProtos.ContainerLifeCycleState.INVALID);

    // Create Close container request
    ContainerCommandRequestProto closeContainerRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CloseContainer)
            .setContainerID(DUMMY_CONTAINER_ID)
            .setDatanodeUuid(DATANODE_UUID)
            .setCloseContainer(ContainerProtos.CloseContainerRequestProto
                .getDefaultInstance())
            .build();
    dispatcher.dispatch(closeContainerRequest);

    Mockito.when(handler.handleCloseContainer(any(), any()))
        .thenCallRealMethod();
    // Closing invalid container should return error response.
    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleCloseContainer(closeContainerRequest, container);

    Assert.assertTrue("Close container should return Invalid container error",
        response.getResult().equals(
            ContainerProtos.Result.INVALID_CONTAINER_STATE));
  }
}

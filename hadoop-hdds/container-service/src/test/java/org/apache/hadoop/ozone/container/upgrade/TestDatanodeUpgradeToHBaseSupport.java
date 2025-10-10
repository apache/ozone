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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.ScmTestMock;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  private void initTests() throws Exception {
    conf = new OzoneConfiguration();
    setup();
  }

  private void setup() throws Exception {
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
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.HADOOP_PRC_PORTS_IN_DATANODEDETAILS.layoutVersion());
    ContainerDispatcher dispatcher = dsm.getContainer().getDispatcher();
    final Pipeline pipeline = MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));

    // Add data to read.
    final long containerID = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    // incremental chunk list should be rejected before finalizing.
    UpgradeTestHelper.putBlock(dispatcher, containerID, pipeline, true, ContainerProtos.Result.UNSUPPORTED_REQUEST);
    Container<?> container = dsm.getContainer().getContainerSet().getContainer(containerID);
    assertEquals(OPEN, container.getContainerData().getState());
    // close container to allow upgrade.
    UpgradeTestHelper.closeContainer(dispatcher, containerID, pipeline);

    dsm.finalizeUpgrade();
    assertTrue(dsm.getLayoutVersionManager().isAllowed(HDDSLayoutFeature.HBASE_SUPPORT));
    // open a new container after finalization
    final long containerID2 = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    // incremental chunk list should work after finalizing.
    UpgradeTestHelper.putBlock(dispatcher, containerID2, pipeline, true);
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
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.HADOOP_PRC_PORTS_IN_DATANODEDETAILS.layoutVersion());
    ContainerDispatcher dispatcher = dsm.getContainer().getDispatcher();
    final Pipeline pipeline = MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));

    // Add data to read.
    final long containerID = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk =
        UpgradeTestHelper.putBlock(dispatcher, containerID, pipeline, false);
    UpgradeTestHelper.finalizeBlock(
        dispatcher, containerID, writeChunk.getBlockID().getLocalID(), ContainerProtos.Result.UNSUPPORTED_REQUEST);
    Container<?> container = dsm.getContainer().getContainerSet().getContainer(containerID);
    assertEquals(OPEN, container.getContainerData().getState());
    // close container to allow upgrade.
    UpgradeTestHelper.closeContainer(dispatcher, containerID, pipeline);

    dsm.finalizeUpgrade();
    assertTrue(dsm.getLayoutVersionManager().isAllowed(HDDSLayoutFeature.HBASE_SUPPORT));
    final long containerID2 = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk2 =
        UpgradeTestHelper.putBlock(dispatcher, containerID2, pipeline, false);
    // Make sure we can read after finalizing too.
    UpgradeTestHelper.finalizeBlock(
        dispatcher, containerID2, writeChunk2.getBlockID().getLocalID(), ContainerProtos.Result.SUCCESS);
    Container<?> container2 = dsm.getContainer().getContainerSet().getContainer(containerID2);
    assertEquals(OPEN, container2.getContainerData().getState());
  }

}

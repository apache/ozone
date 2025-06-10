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
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.ScmTestMock;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.metadata.ContainerCreateInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests upgrading a single datanode from HBASE_SUPPORT to CONTAINERID_TABLE_SCHEMA_CHANGE.
 */
public class TestDatanodeUpgradeToContainerIdsTable {
  @TempDir
  private Path tempFolder;

  private DatanodeStateMachine dsm;
  private ContainerDispatcher dispatcher;
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

  @Test
  public void testContainerTableAccessBeforeAndAfterUpgrade() throws Exception {
    initTests();
    // start DN and SCM
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf, new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.HBASE_SUPPORT.layoutVersion());
    dispatcher = dsm.getContainer().getDispatcher();
    final Pipeline pipeline = MockPipeline.createPipeline(Collections.singletonList(dsm.getDatanodeDetails()));

    // add a container
    final long containerID = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    Container<?> container = dsm.getContainer().getContainerSet().getContainer(containerID);
    assertEquals(OPEN, container.getContainerData().getState());
    // close container to allow upgrade.
    UpgradeTestHelper.closeContainer(dispatcher, containerID, pipeline);

    dsm.finalizeUpgrade();
    assertTrue(dsm.getLayoutVersionManager().isAllowed(HDDSLayoutFeature.CONTAINERID_TABLE_SCHEMA_CHANGE));
    ContainerCreateInfo containerCreateInfo =
        dsm.getContainer().getWitnessedContainerMetadataStore().getContainerIdsTable()
            .get(ContainerID.valueOf(containerID));
    // state is always open as state is update while create container only.
    assertEquals(OPEN, containerCreateInfo.getState());
  }
}

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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.hadoop.hdds.HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.RATIS_DATASTREAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ChainedReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for {@link ChainedPipelineProvider}.
 */
public class TestChainedPipelineProvider extends AbstractNCopyPipelineProviderTest {
  private static final ChainedReplicationConfig CHAINED_THREE =
      ChainedReplicationConfig.getInstance(ReplicationFactor.THREE);

  @TempDir
  private File testDir;
  private DBStore dbStore;
  private ChainedPipelineProvider provider;

  private void init(int numStreamNodes, int numNonStreamNodes) throws Exception {
    init(numStreamNodes, numNonStreamNodes, true);
  }

  private void init(int numStreamNodes, int numNonStreamNodes, boolean chainedPipelinesEnabled) throws Exception {
    final List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < numStreamNodes; i++) {
      nodes.add(createDatanodeDetails(true));
    }
    for (int i = 0; i < numNonStreamNodes; i++) {
      nodes.add(createDatanodeDetails(false));
    }

    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    final MockNodeManager nodeManager = new MockNodeManager(
        new NetworkTopologyImpl(new OzoneConfiguration()), nodes, false, nodes.size());
    nodeManager.setNumPipelinePerDatanode(1);
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    final SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);
    final PipelineStateManager stateManager = PipelineStateManagerImpl.newBuilder()
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
    ScmVersionManager versionManager = mock(ScmVersionManager.class);
    when(versionManager.isAllowed(STREAM_PUTBLOCK_WITHOUT_RAFT)).thenReturn(chainedPipelinesEnabled);
    provider = new ChainedPipelineProvider(nodeManager, stateManager, conf, new EventQueue(),
        SCMContext.emptyContext(), versionManager);
  }

  @AfterEach
  void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  @Test
  public void testCreatePipeline() throws Exception {
    init(3, 3);
    final Pipeline pipeline = provider.create(CHAINED_THREE);
    assertEquals(ReplicationType.CHAINED, pipeline.getType());
    assertEquals(CHAINED_THREE, pipeline.getReplicationConfig());
    assertEquals(Pipeline.PipelineState.ALLOCATED, pipeline.getPipelineState());
    assertEquals(3, pipeline.getNodes().size());
    for (DatanodeDetails dn : pipeline.getNodes()) {
      assertTrue(dn.hasPort(RATIS_DATASTREAM), () -> dn + " does not have " + RATIS_DATASTREAM);
    }
  }

  @Test
  public void testCreatePipelineNoFallbackToNonStreamNodes() throws Exception {
    init(2, 1);
    assertThrows(SCMException.class, () -> provider.create(CHAINED_THREE));
  }

  @Test
  public void testCreatePipelineBeforeUpgradeFinalized() throws Exception {
    init(3, 0, false);
    SCMException exception = assertThrows(SCMException.class, () -> provider.create(CHAINED_THREE));
    assertTrue(exception.getMessage().contains("not finalized"));
    assertTrue(exception.getMessage().contains("CHAINED"));
  }

  @Test
  public void testCreatePipelineUnsupportedFactor() throws Exception {
    init(3, 0);
    assertThrows(IllegalArgumentException.class,
        () -> provider.create(ChainedReplicationConfig.getInstance(ReplicationFactor.ZERO)));
  }
}

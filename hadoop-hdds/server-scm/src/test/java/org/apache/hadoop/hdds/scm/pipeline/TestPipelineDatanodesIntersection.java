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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for pipeline datanodes intersection.
 */
public class TestPipelineDatanodesIntersection {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestPipelineDatanodesIntersection.class.getName());

  private OzoneConfiguration conf;
  private boolean end;
  @TempDir
  private File testDir;
  private DBStore dbStore;

  @BeforeEach
  public void initialize() throws IOException {
    conf = SCMTestUtils.getConf(testDir);
    end = false;
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  @ParameterizedTest
  @CsvSource({
      "4, 5",
      "10, 5",
      "20, 5",
      "50, 5",
      "100, 5",
      "100, 10"
  })
  public void testPipelineDatanodesIntersection(int nodeCount,
      int nodeHeaviness) throws IOException {
    NodeManager nodeManager = new MockNodeManager(true, nodeCount);
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, nodeHeaviness);
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);

    PipelineStateManager stateManager = PipelineStateManagerImpl.newBuilder()
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();


    PipelineProvider<RatisReplicationConfig> provider =
        new MockRatisPipelineProvider(nodeManager, stateManager, conf);

    int healthyNodeCount = nodeManager
        .getNodeCount(NodeStatus.inServiceHealthy());
    int intersectionCount = 0;
    int createdPipelineCount = 0;
    while (!end && createdPipelineCount <= healthyNodeCount * nodeHeaviness) {
      try {
        Pipeline pipeline = provider.create(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE));
        HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(
            ClientVersion.CURRENT_VERSION);
        stateManager.addPipeline(pipelineProto);
        nodeManager.addPipeline(pipeline);
        List<Pipeline> overlapPipelines = RatisPipelineUtils
            .checkPipelineContainSameDatanodes(stateManager, pipeline);

        if (!overlapPipelines.isEmpty()) {
          intersectionCount++;
          for (Pipeline overlapPipeline : overlapPipelines) {
            LOG.info("This pipeline: " + pipeline.getId().toString() +
                " overlaps with previous pipeline: " + overlapPipeline.getId() +
                ". They share same set of datanodes as: " +
                pipeline.getNodesInOrder().get(0).getUuid() + "/" +
                pipeline.getNodesInOrder().get(1).getUuid() + "/" +
                pipeline.getNodesInOrder().get(2).getUuid() + " and " +
                overlapPipeline.getNodesInOrder().get(0).getUuid() + "/" +
                overlapPipeline.getNodesInOrder().get(1).getUuid() + "/" +
                overlapPipeline.getNodesInOrder().get(2).getUuid() +
                " is the same.");
          }
        }
        createdPipelineCount++;
      } catch (SCMException e) {
        end = true;
      } catch (IOException e) {
        end = true;
        // Should not throw regular IOException.
        fail();
      }
    }

    end = false;

    LOG.info("Among total " +
        stateManager
            .getPipelines(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE))
            .size() + " created pipelines" +
        " with " + healthyNodeCount + " healthy datanodes and " +
        nodeHeaviness + " as node heaviness, " +
        intersectionCount + " pipelines has same set of datanodes.");
  }
}

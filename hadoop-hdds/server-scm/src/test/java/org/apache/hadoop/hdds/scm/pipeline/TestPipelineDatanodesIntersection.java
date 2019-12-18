/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE;

/**
 * Test for pipeline datanodes intersection.
 */
@RunWith(Parameterized.class)
public class TestPipelineDatanodesIntersection {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestPipelineDatanodesIntersection.class.getName());

  private int nodeCount;
  private int nodeHeaviness;
  private OzoneConfiguration conf;
  private boolean end;

  @Before
  public void initialize() {
    conf = new OzoneConfiguration();
    end = false;
  }

  public TestPipelineDatanodesIntersection(int nodeCount, int nodeHeaviness) {
    this.nodeCount = nodeCount;
    this.nodeHeaviness = nodeHeaviness;
  }

  @Parameterized.Parameters
  public static Collection inputParams() {
    return Arrays.asList(new Object[][] {
        {4, 5},
        {10, 5},
        {20, 5},
        {50, 5},
        {100, 5},
        {100, 10}
    });
  }

  @Test
  public void testPipelineDatanodesIntersection() {
    NodeManager nodeManager= new MockNodeManager(true, nodeCount);
    conf.setInt(OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT, nodeHeaviness);
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    PipelineStateManager stateManager = new PipelineStateManager(conf);
    PipelineProvider provider = new MockRatisPipelineProvider(nodeManager,
        stateManager, conf);

    int healthyNodeCount = nodeManager
        .getNodeCount(HddsProtos.NodeState.HEALTHY);
    int intersectionCount = 0;
    int createdPipelineCount = 0;
    while (!end && createdPipelineCount <= healthyNodeCount * nodeHeaviness) {
      try {
        Pipeline pipeline = provider.create(HddsProtos.ReplicationFactor.THREE);
        stateManager.addPipeline(pipeline);
        nodeManager.addPipeline(pipeline);
        Pipeline overlapPipeline = RatisPipelineUtils
            .checkPipelineContainSameDatanodes(stateManager, pipeline);
        if (overlapPipeline != null){
          intersectionCount++;
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
        createdPipelineCount++;
      } catch(SCMException e) {
        end = true;
      } catch (IOException e) {
        end = true;
        // Should not throw regular IOException.
        Assert.fail();
      }
    }

    end = false;

    LOG.info("Among total " +
        stateManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE).size() + " created pipelines" +
        " with " + healthyNodeCount + " healthy datanodes and " +
        nodeHeaviness + " as node heaviness, " +
        intersectionCount + " pipelines has same set of datanodes.");
  }
}

/*
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

package org.apache.hadoop.ozone.container.metrics;

import org.apache.commons.text.WordUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeQueueMetrics;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.apache.ozone.test.JUnit5AwareTimeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeQueueMetrics.COMMAND_DISPATCHER_QUEUE_PREFIX;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeQueueMetrics.STATE_CONTEXT_COMMAND_QUEUE_PREFIX;
import static org.apache.ozone.test.MetricsAsserts.getLongGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;

/**
 * Test for queue metrics of datanodes.
 */
public class TestDatanodeQueueMetrics {

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private static int numOfOMs = 3;
  private String scmServiceId;
  private static int numOfSCMs = 3;

  private static final Logger LOG = LoggerFactory
      .getLogger(TestDatanodeQueueMetrics.class);

  @Rule
  public TestRule timeout = new JUnit5AwareTimeout(new Timeout(300_000));

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, "10s");
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    scmServiceId = "scm-service-test1";
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setSCMServiceId(scmServiceId)
        .setNumOfStorageContainerManagers(numOfSCMs)
        .setNumOfOzoneManagers(numOfOMs)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
  }
  /**
    * Set a timeout for each test.
    */

  @Test
  public void testQueueMetrics() {

    for (SCMCommandProto.Type type: SCMCommandProto.Type.values()) {
      Assertions.assertTrue(
          getGauge(STATE_CONTEXT_COMMAND_QUEUE_PREFIX +
              WordUtils.capitalize(String.valueOf(type)) + "Size") >= 0);
      Assertions.assertTrue(
          getGauge(COMMAND_DISPATCHER_QUEUE_PREFIX +
              WordUtils.capitalize(String.valueOf(type)) + "Size") >= 0);
    }

  }

  private long getGauge(String metricName) {
    return getLongGauge(metricName,
        getMetrics(DatanodeQueueMetrics.METRICS_SOURCE_NAME));
  }
}

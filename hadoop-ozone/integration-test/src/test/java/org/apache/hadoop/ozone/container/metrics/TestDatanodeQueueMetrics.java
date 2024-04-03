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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeQueueMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.commons.text.WordUtils.capitalize;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeQueueMetrics.COMMAND_DISPATCHER_QUEUE_PREFIX;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeQueueMetrics.STATE_CONTEXT_COMMAND_QUEUE_PREFIX;
import static org.apache.ozone.test.MetricsAsserts.getLongGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for queue metrics of datanodes.
 */
@Timeout(300)
public class TestDatanodeQueueMetrics {

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String omServiceId;
  private static int numOfOMs = 3;
  private String scmServiceId;
  private static int numOfSCMs = 3;

  private static final Logger LOG = LoggerFactory
      .getLogger(TestDatanodeQueueMetrics.class);

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
    omServiceId = "om-service-test1";
    scmServiceId = "scm-service-test1";
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(omServiceId)
        .setSCMServiceId(scmServiceId)
        .setNumOfStorageContainerManagers(numOfSCMs)
        .setNumOfOzoneManagers(numOfOMs)
        .setNumDatanodes(1);
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
  }
  /**
    * Set a timeout for each test.
    */

  @Test
  public void testQueueMetrics() {
    for (SCMCommandProto.Type type: SCMCommandProto.Type.values()) {
      String typeSize = capitalize(String.valueOf(type)) + "Size";
      assertThat(getGauge(STATE_CONTEXT_COMMAND_QUEUE_PREFIX + typeSize))
          .isGreaterThanOrEqualTo(0);
      assertThat(getGauge(COMMAND_DISPATCHER_QUEUE_PREFIX + typeSize))
          .isGreaterThanOrEqualTo(0);
    }

  }

  private long getGauge(String metricName) {
    return getLongGauge(metricName,
        getMetrics(DatanodeQueueMetrics.METRICS_SOURCE_NAME));
  }
}

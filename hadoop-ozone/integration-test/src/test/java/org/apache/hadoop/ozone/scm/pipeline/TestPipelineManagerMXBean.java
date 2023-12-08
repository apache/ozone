/**
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

package org.apache.hadoop.ozone.scm.pipeline;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;


/**
 * Test cases to verify the metrics exposed by SCMPipelineManager via MXBean.
 */
@Timeout(3000)
public class TestPipelineManagerMXBean {

  private MiniOzoneCluster cluster;
  private MBeanServer mbs;

  @BeforeEach
  public void init()
      throws IOException, TimeoutException, InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    mbs = ManagementFactory.getPlatformMBeanServer();
  }

  /**
   * Verifies SCMPipelineManagerInfo metrics.
   *
   * @throws Exception
   */
  @Test
  public void testPipelineInfo() throws Exception {
    ObjectName bean = new ObjectName(
        "Hadoop:service=SCMPipelineManager,name=SCMPipelineManagerInfo");
    Map<String, Integer> pipelineStateCount = cluster
        .getStorageContainerManager().getPipelineManager().getPipelineInfo();

    GenericTestUtils.waitFor(() -> {
      try {
        final TabularData data = (TabularData) mbs.getAttribute(
            bean, "PipelineInfo");
        for (Map.Entry<String, Integer> entry : pipelineStateCount.entrySet()) {
          final Integer count = entry.getValue();
          final Integer currentCount = getMetricsCount(data, entry.getKey());
          if (currentCount == null || !currentCount.equals(count)) {
            return false;
          }
        }
        return true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 500, 3000);
  }

  private Integer getMetricsCount(TabularData data, String state) {
    for (Object obj : data.values()) {
      CompositeData cds = assertInstanceOf(CompositeData.class, obj);
      if (cds.get("key").equals(state)) {
        return Integer.parseInt(cds.get("value").toString());
      }
    }
    return null;
  }

  @AfterEach
  public void teardown() {
    cluster.shutdown();
  }
}

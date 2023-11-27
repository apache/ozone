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

import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    TabularData data =
        (TabularData) mbs.getAttribute(bean, "PipelineInfo");
    Map<String, Integer> datanodeInfo = cluster.getStorageContainerManager()
        .getPipelineManager().getPipelineInfo();
    verifyEquals(data, datanodeInfo, bean);
  }

  private void verifyEquals(TabularData actualData,
                            Map<String, Integer> expectedData, ObjectName bean)
      throws InterruptedException, TimeoutException {
    assertNotNull(actualData);
    assertNotNull(expectedData);
    for (Object obj : actualData.values()) {
      assertTrue(obj instanceof CompositeData);
      CompositeData cds = (CompositeData) obj;
      assertEquals(2, cds.values().size());
      Iterator<?> it = cds.values().iterator();
      String key = it.next().toString();
      assertTrue(expectedData.containsKey(key));
      // Wait before all pipelines becomes stable in their respective states.
      GenericTestUtils.waitFor(() -> {
        long actualCountForPipelineState =
            getActualCountForPipelineState(key, bean);
        return expectedData.remove(key).longValue() ==
            actualCountForPipelineState;
      }, 500, 3000);
    }
    assertTrue(expectedData.isEmpty());
  }

  private long getActualCountForPipelineState(String pipelineState,
                                              ObjectName bean) {
    TabularData data = null;
    try {
      data = (TabularData) mbs.getAttribute(bean, "PipelineInfo");
    } catch (MBeanException e) {
      throw new RuntimeException(e);
    } catch (AttributeNotFoundException e) {
      throw new RuntimeException(e);
    } catch (InstanceNotFoundException e) {
      throw new RuntimeException(e);
    } catch (ReflectionException e) {
      throw new RuntimeException(e);
    }
    String value = null;
    for (Object obj : data.values()) {
      assertTrue(obj instanceof CompositeData);
      CompositeData cds = (CompositeData) obj;
      Iterator<?> it = cds.values().iterator();
      String key = it.next().toString();
      if (key.equals(pipelineState)) {
        value = it.next().toString();
        break;
      }
    }
    return Long.parseLong(value);
  }

  @AfterEach
  public void teardown() {
    cluster.shutdown();
  }
}

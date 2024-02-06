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

package org.apache.hadoop.hdds.scm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Class which tests the SCMNodeManagerInfo Bean.
 */
@Timeout(300)
public class TestSCMNodeManagerMXBean {

  public static final Logger LOG = LoggerFactory.getLogger(TestSCMMXBean.class);
  private static int numOfDatanodes = 3;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;
  private static MBeanServer mbs;

  @BeforeAll
  public static void init() throws IOException, TimeoutException,
      InterruptedException {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_STALENODE_INTERVAL, "60000ms");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    mbs = ManagementFactory.getPlatformMBeanServer();
  }

  @AfterAll
  public static void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDiskUsage() throws Exception {
    ObjectName bean = new ObjectName(
        "Hadoop:service=SCMNodeManager,"
            + "name=SCMNodeManagerInfo");

    TabularData data = (TabularData) mbs.getAttribute(bean, "NodeInfo");
    Map<String, Long> datanodeInfo = scm.getScmNodeManager().getNodeInfo();
    verifyEquals(data, datanodeInfo);
  }

  @Test
  public void testNodeCount() throws Exception {
    ObjectName bean = new ObjectName(
        "Hadoop:service=SCMNodeManager,"
            + "name=SCMNodeManagerInfo");

    TabularData data = (TabularData) mbs.getAttribute(bean, "NodeCount");
    Map<String, Map<String, Integer>> mbeanMap = convertNodeCountToMap(data);
    Map<String, Map<String, Integer>> nodeMap =
        scm.getScmNodeManager().getNodeCount();
    assertEquals(nodeMap, mbeanMap);
  }

  private Map<String, Map<String, Integer>> convertNodeCountToMap(
      TabularData data) {
    Map<String, Map<String, Integer>> map = new HashMap<>();
    for (Object o : data.values()) {
      CompositeData cds = (CompositeData) o;
      Iterator<?> it = cds.values().iterator();
      String opState = it.next().toString();
      TabularData states = (TabularData) it.next();

      Map<String, Integer> healthStates = new HashMap<>();
      for (Object obj : states.values()) {
        CompositeData stateData = (CompositeData) obj;
        Iterator<?> stateIt = stateData.values().iterator();
        String health = stateIt.next().toString();
        Integer value = Integer.parseInt(stateIt.next().toString());
        healthStates.put(health, value);
      }
      map.put(opState, healthStates);
    }
    return map;
  }

  private void verifyEquals(TabularData actualData, Map<String, Long>
      expectedData) {
    assertNotNull(actualData);
    assertNotNull(expectedData);
    for (Object obj : actualData.values()) {
      CompositeData cds = assertInstanceOf(CompositeData.class, obj);
      assertEquals(2, cds.values().size());
      Iterator<?> it = cds.values().iterator();
      String key = it.next().toString();
      String value = it.next().toString();
      long num = Long.parseLong(value);
      assertThat(expectedData).containsKey(key);
      assertEquals(expectedData.remove(key).longValue(), num);
    }
    assertThat(expectedData).isEmpty();
  }

}

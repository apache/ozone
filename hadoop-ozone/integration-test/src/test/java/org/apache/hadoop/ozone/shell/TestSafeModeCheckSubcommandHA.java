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

package org.apache.hadoop.ozone.shell;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration tests for SafeModeCheckSubcommand in HA mode.
 * Tests the 'ozone admin safemode status' command with SCM HA cluster.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSafeModeCheckSubcommandHA {
  private OzoneAdmin ozoneAdmin;
  private MiniOzoneHAClusterImpl cluster;
  private GenericTestUtils.PrintStreamCapturer out;
  private GenericTestUtils.PrintStreamCapturer err;

  @BeforeAll
  void init() throws IOException, InterruptedException, TimeoutException {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-test")
        .setNumOfOzoneManagers(3)
        .setSCMServiceId("scm-test")
        .setNumOfStorageContainerManagers(3)
        .build();

    cluster.waitForClusterToBeReady();
  }
  
  @BeforeEach
  void setupCapture() {
    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();
    ozoneAdmin = new OzoneAdmin();
    Map<String, String> configOverrides = new HashMap<>();
    cluster.getConf().forEach(entry ->
        configOverrides.put(entry.getKey(), entry.getValue()));

    ozoneAdmin.setConfigurationOverrides(configOverrides);
  }

  @AfterEach
  void stopCapture() {
    IOUtils.closeQuietly(out);
    IOUtils.closeQuietly(err);
  }

  @Test
  public void testNoOptionQueriesLeader() {
    String[] args = {"safemode", "status"};
    ozoneAdmin.execute(args);
    String output = out.get();
    
    assertThat(output).contains(cluster.getScmLeader().getSCMNodeId());
    assertThat(output).containsPattern("SCM is (in|out of) safe mode\\.");
  }

  @Test
  public void testNoOptionWithVerboseShowsRules() {
    String[] args = {"safemode", "status", "--verbose"};
    ozoneAdmin.execute(args);
    String output = out.get();
    
    assertThat(output).contains(cluster.getScmLeader().getSCMNodeId());
    assertThat(output).containsPattern("SCM is (in|out of) safe mode\\.");
    assertAllSafeModeRules(output);
  }

  @Test
  public void testScmOptionSpecificNodeByAddress() {
    // Query each SCM node individually
    List<StorageContainerManager> scms = cluster.getStorageContainerManagers();
    String serviceId = getServiceId();
    for (StorageContainerManager scm : scms) {
      String nodeId = scm.getSCMNodeId();
      String hostPort = cluster.getConf().get("ozone.scm.client.address." + serviceId + "." + nodeId);

      String[] args = {"safemode", "status", "--scm", hostPort};
      ozoneAdmin.execute(args);
      String output = out.get();

      assertThat(output).contains("Service ID: " + serviceId);
      assertThat(output).contains(nodeId);
      assertThat(output).containsPattern("\\[" + nodeId + "\\]: (IN|OUT OF) SAFE MODE");
    }
  }

  @Test
  public void testScmOptionWithVerbose() {
    // Query specific scm node with verbose flag
    StorageContainerManager scm = cluster.getStorageContainerManagers().get(0);
    String nodeId = scm.getSCMNodeId();
    String serviceId = getServiceId();
    String hostPort = cluster.getConf().get("ozone.scm.client.address." + serviceId + "." + nodeId);
    
    String[] args = {"safemode", "status", "--scm", hostPort, "--verbose"};
    ozoneAdmin.execute(args);
    String output = out.get();

    assertThat(output).contains("Service ID: " + serviceId);
    assertThat(output).contains(nodeId);
    assertAllSafeModeRules(output);
  }

  @Test
  public void testAllOptionShowsAllNodes() {
    // Query all nodes in the cluster
    String[] args = {"safemode", "status", "--all"};
    ozoneAdmin.execute(args);
    String output = out.get();
    
    assertThat(output).contains("Service ID: " + getServiceId());
    assertAllScmNodes(output);
  }

  @Test
  public void testAllOptionWithVerboseShowsAllRules() {
    // Query all nodes with verbose flag
    String[] args = {"safemode", "status", "--all", "--verbose"};
    ozoneAdmin.execute(args);
    String output = out.get();
    
    assertThat(output).contains("Service ID: " + getServiceId());
    assertAllScmNodes(output);
    assertAllSafeModeRules(output);
  }

  private String getServiceId() {
    return cluster.getConf().get("ozone.scm.service.ids");
  }
  
  private void assertAllSafeModeRules(String output) {
    assertThat(output).contains("DataNodeSafeModeRule");
    assertThat(output).contains("RatisContainerSafeModeRule");
    assertThat(output).contains("HealthyPipelineSafeModeRule");
    assertThat(output).contains("StateMachineReadyRule");
    assertThat(output).contains("OneReplicaPipelineSafeModeRule");
    assertThat(output).contains("ECContainerSafeModeRule");
  }
  
  private void assertAllScmNodes(String output) {
    List<StorageContainerManager> scms = cluster.getStorageContainerManagers();
    for (StorageContainerManager scm : scms) {
      String nodeId = scm.getSCMNodeId();
      assertThat(output).contains(nodeId);
      assertThat(output).containsPattern("\\[" + nodeId + "\\]: (IN|OUT OF) SAFE MODE");
    }
  }
}

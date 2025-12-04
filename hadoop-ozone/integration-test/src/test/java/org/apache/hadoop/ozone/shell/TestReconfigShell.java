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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.ReconfigurableBase;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.service.DirectoryDeletingService;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration test for {@code ozone admin reconfig} command.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestReconfigShell implements NonHATests.TestCase {

  private OzoneAdmin ozoneAdmin;
  private ReconfigurationHandler reconfigurationHandler;
  private GenericTestUtils.PrintStreamCapturer out;
  private GenericTestUtils.PrintStreamCapturer err;

  @BeforeEach
  void capture() {
    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();
    ozoneAdmin = new OzoneAdmin();
    reconfigurationHandler = cluster().getOzoneManager().getReconfigurationHandler();
  }

  @AfterEach
  void stopCapture() {
    IOUtils.closeQuietly(out);
  }

  @Test
  void testDataNodeGetReconfigurableProperties() {
    for (HddsDatanodeService dn : cluster().getHddsDatanodes()) {
      InetSocketAddress socket = dn.getClientProtocolServer().getClientRpcAddress();
      executeAndAssertProperties(dn.getReconfigurationHandler(), "DATANODE", socket);
    }
  }

  @Test
  void testOzoneManagerGetReconfigurationProperties() {
    OzoneManager om = cluster().getOzoneManager();
    InetSocketAddress socket = om.getOmRpcServerAddr();
    executeAndAssertProperties(om.getReconfigurationHandler(), "OM", socket);
  }

  @Test
  void testDirectoryDeletingServiceIntervalReconfiguration() throws ReconfigurationException,
      InterruptedException, TimeoutException {
    OzoneManager om = cluster().getOzoneManager();
    InetSocketAddress socket = om.getOmRpcServerAddr();
    LogCapturer dirDeletingServiceLog = LogCapturer.captureLogs(DirectoryDeletingService.class);
    LogCapturer reconfigHandlerLog = LogCapturer.captureLogs(ReconfigurationHandler.class);

    String initialInterval = "1m";
    String intervalFromXML = "2m"; //config value set in ozone-site.xml
    long intervalFromXMLInSeconds = TimeUnit.MINUTES.toSeconds(2); //120 seconds

    reconfigurationHandler.reconfigureProperty(OZONE_DIR_DELETING_SERVICE_INTERVAL, initialInterval);
    assertThat(reconfigurationHandler.getConf().get(OZONE_DIR_DELETING_SERVICE_INTERVAL)).isEqualTo(initialInterval);

    //Start the reconfiguration task
    executeAndAssertStart("OM", socket);
    GenericTestUtils.waitFor(() -> reconfigHandlerLog.getOutput().contains("Reconfiguration completed"),
        1000, 20000);
    assertThat(dirDeletingServiceLog.getOutput()).contains(
        String.format("Updating and restarting DirectoryDeletingService with interval %d %s",
            intervalFromXMLInSeconds, TimeUnit.SECONDS.name().toLowerCase()));
    assertThat(reconfigurationHandler.getConf().get(OZONE_DIR_DELETING_SERVICE_INTERVAL)).isEqualTo(intervalFromXML);

    String address = socket.getHostString() + ":" + socket.getPort();
    GenericTestUtils.waitFor(() -> {
      ozoneAdmin.getCmd().execute("reconfig", "--service", "OM", "--address", address, "status");
      String output = out.get();
      return output.contains("finished") &&
          output.contains(String.format("SUCCESS: Changed property %s", OZONE_DIR_DELETING_SERVICE_INTERVAL));
    }, 1000, 20000);
  }

  @Test
  void testStorageContainerManagerGetReconfigurationProperties() {
    StorageContainerManager scm = cluster().getStorageContainerManager();
    executeAndAssertProperties(scm.getReconfigurationHandler(), "SCM", scm.getClientRpcAddress());
  }

  @Test
  void testDatanodeBulkCommand() {
    executeForInServiceDatanodes(cluster().getHddsDatanodes().size());
  }

  @Test
  void testDatanodeBulkCommandWithOutOfServiceNode() throws Exception {
    DatanodeDetails dn = cluster().getHddsDatanodes().get(0).getDatanodeDetails();
    NodeManager nodeManager = cluster().getStorageContainerManager().getScmNodeManager();
    nodeManager.setNodeOperationalState(dn, HddsProtos.NodeOperationalState.DECOMMISSIONING);

    try {
      executeForInServiceDatanodes(cluster().getHddsDatanodes().size() - 1);
    } finally {
      nodeManager.setNodeOperationalState(dn, HddsProtos.NodeOperationalState.IN_SERVICE);
    }
  }

  private void executeAndAssertProperties(
      ReconfigurableBase reconfigurableBase, String service,
      InetSocketAddress socket) {
    String address = socket.getHostString() + ":" + socket.getPort();
    ozoneAdmin.getCmd().execute("reconfig", "--service", service, "--address", address, "properties");
    assertReconfigurablePropertiesOutput(reconfigurableBase.getReconfigurableProperties());
  }

  private void assertReconfigurablePropertiesOutput(Collection<String> expectedProperties) {
    assertThat(err.get()).isEmpty();

    List<String> outs = Arrays.asList(out.get().split(System.lineSeparator()));
    assertThat(outs).containsAll(expectedProperties);
  }

  private void executeForInServiceDatanodes(int expectedCount) {
    StorageContainerManager scm = cluster().getStorageContainerManager();
    ozoneAdmin.getCmd().execute(
        "-D", OZONE_SCM_CLIENT_ADDRESS_KEY + "=" + getAddress(scm.getClientRpcAddress()),
        "reconfig", "--service", "DATANODE", "--in-service-datanodes", "properties");

    assertThat(err.get()).isEmpty();
    assertThat(out.get())
        .contains(String.format("successfully %d ", expectedCount));
  }

  private String getAddress(InetSocketAddress socket) {
    return socket.getHostString() + ":" + socket.getPort();
  }

  private void executeAndAssertStart(String service, InetSocketAddress socket) {
    String address = socket.getHostString() + ":" + socket.getPort();
    ozoneAdmin.getCmd().execute("reconfig", "--service", service, "--address", address, "start");
    assertThat(out.get()).contains(service + ": Started reconfiguration task on node [" + address + "]");
  }

}

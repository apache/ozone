/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.DefaultConfigManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.UUID;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_CHECK_DURATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_REFRESH_DURATION;

/**
 * Test fetching network topology information from SCM.
 */
public class TestGetTopologyInformation {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore store;
  private static OzoneManagerProtocol writeClient;
  private ClassLoader classLoader =
      Thread.currentThread().getContextClassLoader();

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    conf.set(OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_REFRESH_DURATION, "15s");
    conf.set(OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_CHECK_DURATION, "2s");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOmId(omId)
        .setNumOfOzoneManagers(3)
        .setNumOfStorageContainerManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();
    writeClient = store.getClientProxy().getOzoneManagerClient();
  }

  @AfterClass
  public static void stop() {
    if (cluster != null) {
      cluster.stop();
    }
    DefaultConfigManager.clearDefaultConfigs();
  }

  @Test
  public void testGetTopologyInformation()
      throws InterruptedException, IOException {
    String expected =
        conf.get(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE);
    String actual = writeClient.refetchTopologyInformation();
    Assertions.assertEquals(expected, actual);

    testWithUpdatedTopologyInformation();
  }

  /**
   * Modify the path to SCM's network topology schema file to test whether OM
   * refetches the updated file within the specified refresh duration.
   * @throws InterruptedException
   */
  public void testWithUpdatedTopologyInformation()
      throws InterruptedException, IOException {
    String filePath =
        classLoader.getResource("./networkTopologyTestFiles/test-topology.xml")
            .getPath();
    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE, filePath);
    Thread.sleep(30000);
    String expected =
        conf.get(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE);
    String actual = writeClient.refetchTopologyInformation();
    Assertions.assertEquals(expected, actual);
  }
}

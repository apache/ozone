/**
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

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;

/**
 * Test some client operations after cluster starts. And perform restart and
 * then performs client operations and check the behavior is expected or not.
 */
@Timeout(240)
public class TestOzoneManagerReadonlyMode {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static OzoneClient client;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    // Use OBS layout for key rename testing.
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.OBJECT_STORE.name());
    conf.setBoolean(OMConfigKeys.OZONE_OM_ONFAILURE_READONLY, true);
    cluster =  MiniOzoneCluster.newHABuilder(conf).setOMServiceId("om-service-test1")
        .setSCMServiceId("scm-service-test1").setNumOfOzoneManagers(3)
        .setNumOfStorageContainerManagers(1).setNumOfActiveSCMs(1).setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReadonlyModeWhileExceptionAndRestart() throws Exception {
    cluster.stopRecon();
    cluster.getHddsDatanodes().stream().forEach(e -> e.stop());
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());

    // mock PerfMetrics to throw exception when executing request
    for (OzoneManager om : ((MiniOzoneHAClusterImpl) cluster).getOzoneManagersList()) {
      OMPerformanceMetrics pf1 = Mockito.spy(om.getPerfMetrics());
      doThrow(new IllegalArgumentException("test throw")).when(pf1).getValidateAndUpdateCacheLatencyNs();
      om.setPerfMetrics(pf1);
    }

    // volume creation should fail
    try {
      objectStore.createVolume(volumeName + "12");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("Exception occurred and moving to readonly mode"));
    }

    GenericTestUtils.waitFor(() -> {
      for (OzoneManager om : ((MiniOzoneHAClusterImpl) cluster).getOzoneManagersList()) {
        if (om.getOmRatisServer().getOmStateMachine().isReadOnly()) {
          return true;
        }
      }
      return false;
    }, 100, 1000);

    try {
      objectStore.createVolume(volumeName + "1112");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("OM is running in readonly mode"));
    }

    // verify list operation working fine
    for (int i = 0; i < 10; ++i) {
      cluster.newClient().getObjectStore().listVolumes("");
    }

    cluster.restartOzoneManager();

    // restart, verify state machine remains in readonly mode
    GenericTestUtils.waitFor(() -> {
      for (OzoneManager om : ((MiniOzoneHAClusterImpl) cluster).getOzoneManagersList()) {
        if (om.getOmRatisServer().getOmStateMachine().isReadOnly()) {
          return true;
        }
      }
      return false;
    }, 100, 10000);

    try {
      objectStore.createVolume(volumeName + "1112");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("OM is running in readonly mode"));
    }

    for (int i = 0; i < 10; ++i) {
      cluster.newClient().getObjectStore().listVolumes("");
    }
  }
}

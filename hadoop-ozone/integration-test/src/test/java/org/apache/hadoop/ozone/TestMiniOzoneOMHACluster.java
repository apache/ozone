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

package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;

/**
 * This class tests MiniOzoneHAClusterImpl.
 */
public class TestMiniOzoneOMHACluster {

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private int numOfOMs = 3;

  public ExpectedException exception = ExpectedException.none();

  @Timeout(value = 300, unit = TimeUnit.SECONDS)

  /**
   * Create a MiniOzoneHAClusterImpl for testing.
   *
   * @throws IOException
   */
  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "omServiceId1";
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniOzoneHAClusterImpl.
   */
  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetOMLeader() throws InterruptedException, TimeoutException {
    AtomicReference<OzoneManager> ozoneManager = new AtomicReference<>();
    // Wait for OM leader election to finish
    GenericTestUtils.waitFor(() -> {
      OzoneManager om = cluster.getOMLeader();
      ozoneManager.set(om);
      return om != null;
    }, 100, 120000);
    Assertions.assertNotNull(ozoneManager, "Timed out waiting OM leader election to finish: "
            + "no leader or more than one leader.");
    Assertions.assertTrue(ozoneManager.get().isLeaderReady(),
            "Should have gotten the leader!");
  }
}

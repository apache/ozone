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
package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class TestReconWithOzoneManagerHA {
  @Rule
  public Timeout timeout = new Timeout(300_000);

  private MiniOzoneHAClusterImpl cluster;
  private ObjectStore objectStore;
  private final String omServiceId = "omService1";

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, Boolean.TRUE.toString());
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(omServiceId)
        .setNumDatanodes(1)
        .setNumOfOzoneManagers(3)
        .includeRecon(true)
        .setReconHttpPort(NetUtils.getFreeSocketPort())
        .setReconDatanodePort(NetUtils.getFreeSocketPort())
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReconGetsSnapshotFromLeader() throws Exception {
    AtomicReference<OzoneManager> ozoneManager = new AtomicReference<>();
    // Wait for OM leader election to finish
    GenericTestUtils.waitFor(() -> {
      OzoneManager om = cluster.getOMLeader();
      ozoneManager.set(om);
      return om != null;
    }, 100, 120000);
    Assert.assertNotNull("Timed out waiting OM leader election to finish: "
        + "no leader or more than one leader.", ozoneManager);
    Assert.assertTrue("Should have gotten the leader!",
        ozoneManager.get().isLeader());

    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        cluster.getReconServer().getOzoneManagerServiceProvider();

    String hostname =
        ozoneManager.get().getHttpServer().getHttpAddress().getHostName();
    String expectedUrl = "http://" +
        (hostname.equals("0.0.0.0") ? "localhost" : hostname) + ":" +
        ozoneManager.get().getHttpServer().getHttpAddress().getPort() +
        OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;
    String snapshotUrl = impl.getOzoneManagerSnapshotUrl();
    Assert.assertEquals("OM Snapshot should be requested from the leader.",
        expectedUrl, snapshotUrl);
  }
}

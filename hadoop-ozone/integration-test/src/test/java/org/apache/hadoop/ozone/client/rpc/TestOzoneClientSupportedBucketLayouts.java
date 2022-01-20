/**
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.om.upgrade.OMUpgradeFinalizer;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.STARTING_FINALIZATION;

/**
 * Client RPC test to validate supported bucket layouts during upgrades.
 */
public class TestOzoneClientSupportedBucketLayouts {
  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(30);

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private static OzoneClient client;

  /**
   * Create a MiniOzoneCluster for testing.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneClientConfig config = new OzoneClientConfig();
    conf.setFromObject(config);

    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
  }

  /**
   * Shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * The OM should return an empty list supported layouts when it is in
   * pre-finalize state.
   * When the upgrade is finalized, the OM should return the list of supported
   * bucket layouts.
   *
   * @throws Exception
   */
  @Test
  public void testSupportedBucketLayoutsDuringUpgrade() throws Exception {
    OzoneManager ozoneManager = cluster.getOzoneManager();
    OMLayoutVersionManager versionManager = ozoneManager.getVersionManager();

    // Transition into pre-finalization state.
    versionManager.setUpgradeState(FINALIZATION_REQUIRED);

    ClientProtocol proxy = client.getObjectStore().getClientProxy();

    // Supported Bucket Layouts list should be empty in pre-finalization state.
    Assert.assertEquals(0, proxy.getSupportedBucketLayouts().size());

    // finalize the upgrade.
    versionManager.setUpgradeState(STARTING_FINALIZATION);
    OMUpgradeFinalizer finalizer = new OMUpgradeFinalizer(versionManager);
    finalizer.finalize(ozoneManager.getOMNodeId(), ozoneManager);

    // Wait for the upgrade to finalize.
    GenericTestUtils.waitFor(
        () -> versionManager.getUpgradeState() != FINALIZATION_DONE, 100,
        20000);

    // Get the list of supported bucket layouts from OM.
    List<BucketLayout> supportedLayouts =
        ozoneManager.getSupportedBucketLayouts();

    // After finalization, OM should support OBS and FSO bucket layouts.
    Assert.assertEquals(2, supportedLayouts.size());
    Assert.assertTrue(supportedLayouts.contains(BucketLayout.OBJECT_STORE));
    Assert.assertTrue(
        supportedLayouts.contains(BucketLayout.FILE_SYSTEM_OPTIMIZED));
  }
}

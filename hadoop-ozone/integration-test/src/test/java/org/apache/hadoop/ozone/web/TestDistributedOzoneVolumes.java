/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.web;

import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Assert;

import org.junit.rules.Timeout;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Test ozone volume in the distributed storage handler scenario.
 */
public class TestDistributedOzoneVolumes extends TestOzoneHelper {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(TestDistributedOzoneVolumes.class);
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static MiniOzoneClassicCluster cluster = null;
  private static int port = 0;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "distributed"
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);
    conf.set(OzoneConfigKeys.OZONE_HANDLER_TYPE_KEY,
        OzoneConsts.OZONE_HANDLER_DISTRIBUTED);
    cluster = new MiniOzoneClassicCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    DataNode dataNode = cluster.getDataNodes().get(0);
    port = MiniOzoneClassicCluster.getOzoneRestPort(dataNode);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Creates Volumes on Ozone Store.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumes() throws IOException {
    super.testCreateVolumes(port);
    Assert.assertEquals(0, cluster.getKeySpaceManager()
        .getMetrics().getNumVolumeCreateFails());
  }

  /**
   * Create Volumes with Quota.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumesWithQuota() throws IOException {
    super.testCreateVolumesWithQuota(port);
    Assert.assertEquals(0, cluster.getKeySpaceManager()
        .getMetrics().getNumVolumeCreateFails());
  }

  /**
   * Create Volumes with Invalid Quota.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumesWithInvalidQuota() throws IOException {
    super.testCreateVolumesWithInvalidQuota(port);
    Assert.assertEquals(0, cluster.getKeySpaceManager()
        .getMetrics().getNumVolumeCreateFails());
  }

  /**
   * To create a volume a user name must be specified using OZONE_USER header.
   * This test verifies that we get an error in case we call without a OZONE
   * user name.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumesWithInvalidUser() throws IOException {
    super.testCreateVolumesWithInvalidUser(port);
    Assert.assertEquals(0, cluster.getKeySpaceManager()
        .getMetrics().getNumVolumeCreateFails());
  }

  /**
   * Only Admins can create volumes in Ozone. This test uses simple userauth as
   * backend and hdfs and root are admin users in the simple backend.
   * <p>
   * This test tries to create a volume as user bilbo.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumesWithOutAdminRights() throws IOException {
    super.testCreateVolumesWithOutAdminRights(port);
    Assert.assertEquals(0, cluster.getKeySpaceManager()
        .getMetrics().getNumVolumeCreateFails());
  }

  /**
   * Create a bunch of volumes in a loop.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumesInLoop() throws IOException {
    super.testCreateVolumesInLoop(port);
    Assert.assertEquals(0, cluster.getKeySpaceManager()
        .getMetrics().getNumVolumeCreateFails());
  }
  /**
   * Get volumes owned by the user.
   *
   * @throws IOException
   */
  public void testGetVolumesByUser() throws IOException {
    testGetVolumesByUser(port);
  }

  /**
   * Admins can read volumes belonging to other users.
   *
   * @throws IOException
   */
  public void testGetVolumesOfAnotherUser() throws IOException {
    super.testGetVolumesOfAnotherUser(port);
  }

  /**
   * if you try to read volumes belonging to another user,
   * then server always ignores it.
   *
   * @throws IOException
   */
  public void testGetVolumesOfAnotherUserShouldFail() throws IOException {
    super.testGetVolumesOfAnotherUserShouldFail(port);
  }
}

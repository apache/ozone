package org.apache.hadoop.ozone.reconfig;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Tests for OM Reconfigure.
 */
public class TestOmReconfigure {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(300000);
  private OzoneConfiguration conf;
  private MiniOzoneHAClusterImpl cluster;
  private OzoneManager ozoneManager;

  /**
   * Create a MiniDFSCluster for testing.
   */
  @Before
  public void setup() throws Exception {

    conf = new OzoneConfiguration();
    String omServiceId = UUID.randomUUID().toString();
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(3)
        .setNumDatanodes(1)
        .build();

    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();

  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test reconfigure om "ozone.administrators".
   */
  @Test
  public void testOmAdminUsersReconfigure() throws Exception {
    String userA = "mockUserA";
    String userB = "mockUserB";
    conf.set(OZONE_ADMINISTRATORS, userA);
    ozoneManager.reconfigurePropertyImpl(OZONE_ADMINISTRATORS, userA);
    assertTrue(userA + " should be an admin user",
        ozoneManager.getOmAdminUsernames().contains(userA));

    conf.set(OZONE_ADMINISTRATORS, userB);
    ozoneManager.reconfigurePropertyImpl(OZONE_ADMINISTRATORS, userB);
    assertFalse(userA + " should NOT be an admin user",
        ozoneManager.getOmAdminUsernames().contains(userA));
    assertTrue(userB + " should be an admin user",
        ozoneManager.getOmAdminUsernames().contains(userB));
  }

  /**
   * Test reconfigure om "ozone.readonly.administrators".
   */
  @Test
  public void testOmReadOnlyUsersReconfigure() throws Exception {
    String userA = "mockUserA";
    String userB = "mockUserB";
    conf.set(OZONE_READONLY_ADMINISTRATORS, userA);
    ozoneManager.reconfigurePropertyImpl(OZONE_READONLY_ADMINISTRATORS, userA);
    assertTrue(userA + " should be a readOnly admin user",
        ozoneManager.getOmReadOnlyAdminUsernames().contains(userA));

    conf.set(OZONE_READONLY_ADMINISTRATORS, userB);
    ozoneManager.reconfigurePropertyImpl(OZONE_READONLY_ADMINISTRATORS, userB);
    assertFalse(userA + " should NOT be a admin user",
        ozoneManager.getOmReadOnlyAdminUsernames().contains(userA));
    assertTrue(userB + " should be a admin user",
        ozoneManager.getOmReadOnlyAdminUsernames().contains(userB));
  }
}

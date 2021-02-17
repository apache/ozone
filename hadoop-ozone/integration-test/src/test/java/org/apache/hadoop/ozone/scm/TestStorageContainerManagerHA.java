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

package org.apache.hadoop.ozone.scm;

import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.Rule;

import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.UUID;

/**
 * Base class for Ozone Manager HA tests.
 */
public class TestStorageContainerManagerHA {

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private static int numOfOMs = 3;
  private String scmServiceId;
  // TODO: 3 servers will not work until SCM HA configs are considered in
  // SCM HA Ratis server init
  private static int numOfSCMs = 1;


  @Rule
  public Timeout timeout = new Timeout(300_000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    scmServiceId = "scm-service-test1";

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setSCMServiceId(scmServiceId)
        .setNumOfStorageContainerManagers(numOfSCMs)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();
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

  @Test
  public void testAllSCMAreRunning() throws Exception {
    int count = 0;
    List<StorageContainerManager> scms = cluster.getStorageContainerManagers();
    for (StorageContainerManager scm : scms) {
      if (scm.checkLeader()) {
        count++;
      }
    }
    Assert.assertEquals(1, count);
  }
}

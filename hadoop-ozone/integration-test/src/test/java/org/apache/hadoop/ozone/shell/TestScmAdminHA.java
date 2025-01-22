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
package org.apache.hadoop.ozone.shell;

import java.net.InetSocketAddress;

import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * This class tests ozone admin scm commands.
 */
public class TestScmAdminHA {
  private static OzoneAdmin ozoneAdmin;
  private static OzoneConfiguration conf;
  private static String omServiceId;
  private static int numOfOMs;
  private static MiniOzoneCluster cluster;

  @BeforeAll
  public static void init() throws Exception {
    ozoneAdmin = new OzoneAdmin();
    conf = new OzoneConfiguration();

    // Init HA cluster
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    conf.setQuietMode(false);
    // enable ratis for Scm.
    conf.setBoolean(ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY, true);
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetRatisRoles() {
    InetSocketAddress address =
        cluster.getStorageContainerManager().getClientRpcAddress();
    String hostPort = address.getHostName() + ":" + address.getPort();
    String[] args = {"--scm", hostPort, "scm", "roles"};
    ozoneAdmin.execute(args);
  }
}

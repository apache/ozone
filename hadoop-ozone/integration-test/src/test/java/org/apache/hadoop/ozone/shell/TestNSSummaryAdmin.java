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

import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;

/**
 * Test for Namespace CLI.
 */
public class TestNSSummaryAdmin {

  private static OzoneAdmin ozoneAdmin;
  private static OzoneConfiguration conf;
  private static MiniOzoneCluster cluster;

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    TestOMRequestUtils.configureFSOptimizedPaths(conf, true);
    conf.set(OZONE_RECON_ADDRESS_KEY, "localhost:9888");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .withoutDatanodes().includeRecon(true).build();
    cluster.waitForClusterToBeReady();
    // Client uses server conf for this test
    ozoneAdmin = new OzoneAdmin(conf);
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 60000)
  public void testNSSummaryCLI() {
    String[] summaryArgs = {"namespace", "summary", "/"};
    String[] duArgs = {"namespace", "du", "/"};
    String[] duArgsWithOps = {"namespace", "du", "-rfn", "--length=100", "/"};
    String[] quotaArgs = {"namespace", "quota", "/"};
    String[] distArgs = {"namespace", "dist", "/"};

    ozoneAdmin.execute(summaryArgs);
    ozoneAdmin.execute(duArgs);
    ozoneAdmin.execute(duArgsWithOps);
    ozoneAdmin.execute(quotaArgs);
    ozoneAdmin.execute(distArgs);
  }
}

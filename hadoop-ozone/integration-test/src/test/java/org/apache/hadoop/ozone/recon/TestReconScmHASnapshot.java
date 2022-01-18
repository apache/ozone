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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY;
import static org.junit.Assert.assertTrue;

/**
 * Test Recon SCM HA Snapshot Download implementation.
 */
public class TestReconScmHASnapshot {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(100);
  private static OzoneConfiguration conf;
  private static MiniOzoneCluster ozoneCluster;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SCM_HA_ENABLE_KEY, true);
    conf.setBoolean(
        ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_ENABLED, true);
    conf.setInt(ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_THRESHOLD, 0);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 5);
    ozoneCluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(4)
        .includeRecon(true)
        .build();
    ozoneCluster.waitForClusterToBeReady();
  }

  @Test
  public void testScmHASnapshot() throws Exception {
    GenericTestUtils.LogCapturer logCapture = GenericTestUtils.LogCapturer
        .captureLogs(LoggerFactory.getLogger(
            StorageContainerServiceProviderImpl.class));
    TestReconScmSnapshot.testSnapshot(ozoneCluster);
    assertTrue(logCapture.getOutput()
        .contains("Downloaded SCM Snapshot from Leader SCM"));
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (ozoneCluster != null) {
      ozoneCluster.shutdown();
    }
  }
}

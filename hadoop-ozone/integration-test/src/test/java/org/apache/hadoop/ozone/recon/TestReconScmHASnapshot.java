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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY;

/**
 * Test Recon SCM HA Snapshot Download implementation.
 */
@Timeout(300)
public class TestReconScmHASnapshot {
  private OzoneConfiguration conf;
  private MiniOzoneCluster ozoneCluster = null;

  @BeforeEach
  public void setup() throws Exception {
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
    TestReconScmSnapshot.testSnapshot(ozoneCluster);
  }

  @AfterEach
  public void shutdown() throws Exception {
    if (ozoneCluster != null) {
      ozoneCluster.shutdown();
    }
  }
}

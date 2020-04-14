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

package org.apache.hadoop.ozone.om;

import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Test OzoneManager setOwner command.
 */
public class TestOzoneManagerSetOwner {

  @Rule
  public Timeout timeout = new Timeout(120_000);

  /**
   * Start a MiniDFSCluster for testing.
   */
  private MiniOzoneCluster startCluster() throws Exception {

    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    conf.set(OZONE_ADMINISTRATORS, "user1");
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);

    // Fail fast
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 3);
    conf.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 2);
    conf.setInt(IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 200);

    // Use native impl here, default impl doesn't do actual checks
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    // Note: OM doesn't support live config reloading
    conf.setBoolean(OZONE_ACL_ENABLED, true);

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId).setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();

    return cluster;
  }

  private void stopCluster(MiniOzoneCluster cluster) {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSetOwner() throws Exception {
    MiniOzoneCluster cluster = startCluster();
    OzoneClient client = cluster.getClient();
    ObjectStore objectStore = client.getObjectStore();
    ClientProtocol proxy = objectStore.getClientProxy();

    String volumeName = "volume1";
    String ownerName = "user1";
    objectStore.createVolume(volumeName);

    proxy.setVolumeOwner(volumeName, ownerName);
    // Set owner again, expect no NPEs.
    proxy.setVolumeOwner(volumeName, ownerName);

    stopCluster(cluster);
  }

}

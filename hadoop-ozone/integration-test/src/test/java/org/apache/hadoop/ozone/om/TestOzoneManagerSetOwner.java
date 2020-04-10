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

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Test OzoneManager list volume operation under combinations of configs.
 */
public class TestOzoneManagerSetOwner {

  @Rule
  public Timeout timeout = new Timeout(120_000);

  private UserGroupInformation loginUser;

  @Before
  public void init() throws Exception {
    loginUser = UserGroupInformation.getLoginUser();
  }

  /**
   * Create a MiniDFSCluster for testing.
   */
  private MiniOzoneCluster startCluster(boolean aclEnabled) throws Exception {

    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    conf.set(OZONE_ADMINISTRATORS, "user1");
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);

    // Use native impl here, default impl doesn't do actual checks
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    // Note: OM doesn't support live config reloading
    conf.setBoolean(OZONE_ACL_ENABLED, aclEnabled);

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId).setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();

    // loginUser is the user running this test.
    // Implication: loginUser is automatically added to the OM admin list.
    UserGroupInformation.setLoginUser(loginUser);

    return cluster;
  }

  private void stopCluster(MiniOzoneCluster cluster) {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void createVolumeThenSetOwnerTwice(ObjectStore objectStore,
      String volumeName, String ownerName)
      throws IOException {
    ClientProtocol proxy = objectStore.getClientProxy();
    objectStore.createVolume(volumeName);
    proxy.setVolumeOwner(volumeName, ownerName);
    try {
      proxy.setVolumeOwner(volumeName, ownerName);
    } catch (OMException ex) {
      // Expect OMException if setting volume owner to the same user
      if (ex.getResult() != OMException.ResultCodes.ACCESS_DENIED) {
        throw ex;
      }
    }
  }

  @Test
  public void testSetOwner() throws Exception {
    // ozone.acl.enabled = true
    MiniOzoneCluster cluster = startCluster(true);
    // Create volumes with non-default owner
    OzoneClient client = cluster.getClient();
    ObjectStore objectStore = client.getObjectStore();

    // TODO: Expect to fail here before patching OMVolumeSetOwnerRequest.
    createVolumeThenSetOwnerTwice(objectStore, "volume1", "user1");

    stopCluster(cluster);
  }

}

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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Test OzoneManager list volume operation under combinations of configs.
 */
public class TestOzoneManagerListVolumes {

  @Rule
  public Timeout timeout = Timeout.seconds(120);

  private UserGroupInformation adminUser =
      UserGroupInformation.createUserForTesting("om", new String[]{"ozone"});
  private UserGroupInformation user1 =
      UserGroupInformation.createUserForTesting("user1", new String[]{"test"});
  private UserGroupInformation user2 =
      UserGroupInformation.createUserForTesting("user2", new String[]{"test"});

  @Before
  public void init() throws Exception {
    // loginUser is the user running this test.
    // Implication: loginUser is automatically added to the OM admin list.
    UserGroupInformation.setLoginUser(adminUser);
  }

  /**
   * Create a MiniDFSCluster for testing.
   */
  private MiniOzoneCluster startCluster(boolean aclEnabled,
      boolean volListAllAllowed) throws Exception {

    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);

    // Use native impl here, default impl doesn't do actual checks
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    // Note: OM doesn't support live config reloading
    conf.setBoolean(OZONE_ACL_ENABLED, aclEnabled);
    conf.setBoolean(OZONE_OM_VOLUME_LISTALL_ALLOWED, volListAllAllowed);

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId).setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();

    // Create volumes with non-default owners and ACLs
    OzoneClient client = cluster.getClient();
    ObjectStore objectStore = client.getObjectStore();

    /* r = READ, w = WRITE, c = CREATE, d = DELETE
       l = LIST, a = ALL, n = NONE, x = READ_ACL, y = WRITE_ACL */
    String aclUser1All = "user:user1:a";
    String aclUser2All = "user:user2:a";
    String aclWorldAll = "world::a";
    createVolumeWithOwnerAndAcl(objectStore, "volume1", "user1", aclUser1All);
    createVolumeWithOwnerAndAcl(objectStore, "volume2", "user2", aclUser2All);
    createVolumeWithOwnerAndAcl(objectStore, "volume3", "user1", aclUser2All);
    createVolumeWithOwnerAndAcl(objectStore, "volume4", "user2", aclUser1All);
    createVolumeWithOwnerAndAcl(objectStore, "volume5", "user1", aclWorldAll);

    return cluster;
  }

  private void stopCluster(MiniOzoneCluster cluster) {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void createVolumeWithOwnerAndAcl(ObjectStore objectStore,
      String volumeName, String ownerName, String aclString)
      throws IOException {
    ClientProtocol proxy = objectStore.getClientProxy();
    objectStore.createVolume(volumeName);
    proxy.setVolumeOwner(volumeName, ownerName);
    setVolumeAcl(objectStore, volumeName, aclString);
  }

  /**
   * Helper function to set volume ACL.
   */
  private void setVolumeAcl(ObjectStore objectStore, String volumeName,
      String aclString) throws IOException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.VOLUME).setStoreType(OZONE).build();
    Assert.assertTrue(objectStore.setAcl(obj, OzoneAcl.parseAcls(aclString)));
  }

  /**
   * Helper function to reduce code redundancy for test checks with each user
   * under different config combination.
   */
  private void checkUser(MiniOzoneCluster cluster, UserGroupInformation user,
      List<String> expectVol, boolean expectListAllSuccess) throws IOException {

    OzoneClient client = cluster.getClient();
    ObjectStore objectStore = client.getObjectStore();

    // `ozone sh volume list` shall return volumes with LIST permission of user.
    Iterator<? extends OzoneVolume> it = objectStore.listVolumesByUser(
        user.getUserName(), "", "");
    Set<String> accessibleVolumes = new HashSet<>();
    while (it.hasNext()) {
      OzoneVolume vol = it.next();
      String volumeName = vol.getName();
      accessibleVolumes.add(volumeName);
    }
    Assert.assertEquals(new HashSet<>(expectVol), accessibleVolumes);

    // `ozone sh volume list --all` returns all volumes,
    //  or throws exception (for non-admin if acl enabled & listall disallowed).
    if (expectListAllSuccess) {
      it = objectStore.listVolumes("volume");
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      Assert.assertEquals(5, count);
    } else {
      try {
        objectStore.listVolumes("volume");
        Assert.fail("listAllVolumes should fail for " + user.getUserName());
      } catch (RuntimeException ex) {
        // Current listAllVolumes throws RuntimeException
        if (ex.getCause() instanceof OMException) {
          // Expect PERMISSION_DENIED
          if (((OMException) ex.getCause()).getResult() !=
              OMException.ResultCodes.PERMISSION_DENIED) {
            throw ex;
          }
        } else {
          throw ex;
        }
      }
    }
  }

  /**
   * Check if listVolume of other users than the login user works as expected.
   * ozone.om.volume.listall.allowed = true
   * Everyone should be able to list other users' volumes with this config.
   */
  @Test
  public void testListVolumeWithOtherUsersListAllAllowed() throws Exception {
    // ozone.acl.enabled = true, ozone.om.volume.listall.allowed = true
    MiniOzoneCluster cluster = startCluster(true, true);

    // Login as user1, list other users' volumes
    UserGroupInformation.setLoginUser(user1);
    checkUser(cluster, user2, Arrays.asList("volume2", "volume3", "volume4",
        "volume5"), true);

    // Add "s3v" created default by OM.
    checkUser(cluster, adminUser, Arrays.asList("volume1", "volume2", "volume3",
        "volume4", "volume5", "s3v"), true);

    UserGroupInformation.setLoginUser(user2);
    checkUser(cluster, user1, Arrays.asList("volume1", "volume3", "volume4",
        "volume5"), true);
    checkUser(cluster, adminUser, Arrays.asList("volume1", "volume2", "volume3",
        "volume4", "volume5", "s3v"), true);

    stopCluster(cluster);
  }

  /**
   * Check if listVolume of other users than the login user works as expected.
   * ozone.om.volume.listall.allowed = false
   * Only admin should be able to list other users' volumes with this config.
   */
  @Test
  public void testListVolumeWithOtherUsersListAllDisallowed() throws Exception {
    // ozone.acl.enabled = true, ozone.om.volume.listall.allowed = false
    MiniOzoneCluster cluster = startCluster(true, false);

    // Login as user1, list other users' volumes, expect failure
    UserGroupInformation.setLoginUser(user1);
    checkUser(cluster, user2, Arrays.asList("volume2", "volume3", "volume4",
        "volume5"), false);
    // Add "s3v" created default by OM.
    checkUser(cluster, adminUser, Arrays.asList("volume1", "volume2", "volume3",
        "volume4", "volume5", "s3v"), false);

    // While admin should be able to list volumes just fine.
    UserGroupInformation.setLoginUser(adminUser);
    checkUser(cluster, user1, Arrays.asList("volume1", "volume3", "volume4",
        "volume5"), true);
    checkUser(cluster, user2, Arrays.asList("volume2", "volume3", "volume4",
        "volume5"), true);

    stopCluster(cluster);
  }

  @Test
  public void testAclEnabledListAllAllowed() throws Exception {
    // ozone.acl.enabled = true, ozone.om.volume.listall.allowed = true
    MiniOzoneCluster cluster = startCluster(true, true);
    checkUser(cluster, user1, Arrays.asList("volume1", "volume3", "volume4",
        "volume5"), true);
    checkUser(cluster, user2, Arrays.asList("volume2", "volume3", "volume4",
        "volume5"), true);

    // Add "s3v" created default by OM.
    checkUser(cluster, adminUser, Arrays.asList("volume1", "volume2", "volume3",
        "volume4", "volume5", "s3v"), true);
    stopCluster(cluster);
  }

  @Test
  public void testAclEnabledListAllDisallowed() throws Exception {
    // ozone.acl.enabled = true, ozone.om.volume.listall.allowed = false
    MiniOzoneCluster cluster = startCluster(true, false);
    // The default user is adminUser as set in init(),
    // listall always succeeds if we use that UGI, we should use non-admin here
    UserGroupInformation.setLoginUser(user1);
    checkUser(cluster, user1, Arrays.asList("volume1", "volume3", "volume4",
        "volume5"), false);
    UserGroupInformation.setLoginUser(user2);
    checkUser(cluster, user2, Arrays.asList("volume2", "volume3", "volume4",
        "volume5"), false);
    UserGroupInformation.setLoginUser(adminUser);
    // Add "s3v" created default by OM.
    checkUser(cluster, adminUser, Arrays.asList("volume1", "volume2",
        "volume3", "volume4", "volume5", "s3v"), true);
    stopCluster(cluster);
  }

  @Test
  public void testAclDisabledListAllAllowed() throws Exception {
    // ozone.acl.enabled = false, ozone.om.volume.listall.allowed = true
    MiniOzoneCluster cluster = startCluster(false, true);
    checkUser(cluster, user1, Arrays.asList("volume1", "volume3", "volume5"),
        true);
    checkUser(cluster, user2, Arrays.asList("volume2", "volume4"),
        true);
    stopCluster(cluster);
  }

  @Test
  public void testAclDisabledListAllDisallowed() throws Exception {
    // ozone.acl.enabled = false, ozone.om.volume.listall.allowed = false
    MiniOzoneCluster cluster = startCluster(false, false);
    // If ACL is disabled, all permission checks are disabled in Ozone by design
    UserGroupInformation.setLoginUser(user1);
    checkUser(cluster, user1, Arrays.asList("volume1", "volume3", "volume5"),
        true);
    UserGroupInformation.setLoginUser(user2);
    checkUser(cluster, user2, Arrays.asList("volume2", "volume4"),
        true);  // listall will succeed since acl is disabled
    stopCluster(cluster);
  }
}

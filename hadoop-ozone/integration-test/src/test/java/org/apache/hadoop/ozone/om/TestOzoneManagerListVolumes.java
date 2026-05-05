/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.apache.ozone.test.ConfigAssumptions.assumeConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.AclTests;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test OzoneManager list volume operation under combinations of configs.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOzoneManagerListVolumes implements NonHATests.TestCase {

  private static final String UNIQUE = UUID.randomUUID().toString();
  private static final String VOL_PREFIX = "vol-" + UNIQUE;
  private static final String VOLUME_1 = VOL_PREFIX + 1;
  private static final String VOLUME_2 = VOL_PREFIX + 2;
  private static final String VOLUME_3 = VOL_PREFIX + 3;
  private static final String VOLUME_4 = VOL_PREFIX + 4;
  private static final String VOLUME_5 = VOL_PREFIX + 5;

  public static final String USER_1 = "user1-" + UNIQUE;
  private static UserGroupInformation user1 =
      UserGroupInformation.createUserForTesting(USER_1, new String[]{"test"});
  public static final String USER_2 = "user2-" + UNIQUE;
  private static UserGroupInformation user2 =
      UserGroupInformation.createUserForTesting(USER_2, new String[]{"test"});

  // Typycal kerberos user, with shortname different from username.
  private static UserGroupInformation user3 =
      UserGroupInformation.createUserForTesting("user3-" + UNIQUE + "@example.com",
          new String[]{"test"});

  @BeforeEach
  void loginAdmin() {
    // loginUser is the user running this test.
    // Implication: loginUser is automatically added to the OM admin list.
    UserGroupInformation.setLoginUser(AclTests.ADMIN_UGI);
  }

  @AfterEach
  void logout() {
    UserGroupInformation.setLoginUser(null);
    setListAllVolumesAllowed(OmConfig.Defaults.LIST_ALL_VOLUMES_ALLOWED);
  }

  @BeforeAll
  void createVolumes() throws IOException {
    loginAdmin();

    // Create volumes with non-default owners and ACLs
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore objectStore = client.getObjectStore();

      /* r = READ, w = WRITE, c = CREATE, d = DELETE
         l = LIST, a = ALL, n = NONE, x = READ_ACL, y = WRITE_ACL */
      String aclUser1All = userAllACL(USER_1);
      String aclUser2All = userAllACL(USER_2);
      String aclWorldAll = "world::a";
      createVolumeWithOwnerAndAcl(objectStore, VOLUME_1, USER_1, aclUser1All);
      createVolumeWithOwnerAndAcl(objectStore, VOLUME_2, USER_2, aclUser2All);
      createVolumeWithOwnerAndAcl(objectStore, VOLUME_3, USER_1, aclUser2All);
      createVolumeWithOwnerAndAcl(objectStore, VOLUME_4, USER_2, aclUser1All);
      createVolumeWithOwnerAndAcl(objectStore, VOLUME_5, USER_1, aclWorldAll);
    }
  }

  private static String userAllACL(String user) {
    return "user:" + user + ":a";
  }

  private void assumeAclEnabled(boolean expected) {
    assumeConfig(cluster().getConf(),
        OZONE_ACL_ENABLED, OZONE_ACL_ENABLED_DEFAULT, expected);
  }

  private void setListAllVolumesAllowed(boolean newValue) {
    cluster().getOzoneManager().getConfig().setListAllVolumesAllowed(newValue);
  }

  private static void createVolumeWithOwnerAndAcl(ObjectStore objectStore,
      String volumeName, String ownerName, String aclString)
      throws IOException {
    ClientProtocol proxy = objectStore.getClientProxy();
    objectStore.createVolume(volumeName);
    proxy.setVolumeOwner(volumeName, ownerName);
    setVolumeAcl(objectStore, volumeName, aclString);
  }

  private void checkUser(UserGroupInformation user,
                         List<String> expectVol, boolean expectListAllSuccess)
          throws IOException {
    checkUser(user, expectVol, expectListAllSuccess, true);
  }

  /**
   * Helper function to set volume ACL.
   */
  private static void setVolumeAcl(ObjectStore objectStore, String volumeName,
                                   String aclString) throws IOException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.VOLUME).setStoreType(OZONE).build();
    assertTrue(objectStore.setAcl(obj, OzoneAcl.parseAcls(aclString)));
  }

  /**
   * Helper function to reduce code redundancy for test checks with each user
   * under different config combination.
   */
  private void checkUser(UserGroupInformation user,
      List<String> expectVol, boolean expectListAllSuccess,
                         boolean expectListByUserSuccess) throws IOException {
    try (OzoneClient client = cluster().newClient()) {
      checkUser(client, user,
          expectVol, expectListAllSuccess, expectListByUserSuccess);
    }
  }

  private static void checkUser(OzoneClient client, UserGroupInformation user,
      List<String> expectVol, boolean expectListAllSuccess,
      boolean expectListByUserSuccess) throws IOException {

    ObjectStore objectStore = client.getObjectStore();

    // `ozone sh volume list` shall return volumes with LIST permission of user.
    Iterator<? extends OzoneVolume> it;
    try {
      it = objectStore.listVolumesByUser(
              user.getUserName(), "", "");
      Set<String> accessibleVolumes = new HashSet<>();
      while (it.hasNext()) {
        OzoneVolume vol = it.next();
        String volumeName = vol.getName();
        accessibleVolumes.add(volumeName);
      }
      assertThat(accessibleVolumes)
          .containsAll(expectVol);
    } catch (RuntimeException ex) {
      if (expectListByUserSuccess) {
        throw ex;
      }
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


    // `ozone sh volume list --all` returns all volumes,
    //  or throws exception (for non-admin if acl enabled & listall disallowed).
    if (expectListAllSuccess) {
      it = objectStore.listVolumes(VOL_PREFIX);
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      assertEquals(5, count);
    } else {
      RuntimeException ex =
          assertThrows(RuntimeException.class, () -> objectStore.listVolumes(VOL_PREFIX));
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


  /**
   * Check if listVolume of other users than the login user works as expected.
   * ozone.om.volume.listall.allowed = true
   * Everyone should be able to list other users' volumes with this config.
   */
  @Test
  public void testListVolumeWithOtherUsersListAllAllowed() throws Exception {
    // ozone.acl.enabled = true, ozone.om.volume.listall.allowed = true
    assumeAclEnabled(true);
    setListAllVolumesAllowed(true);

    // Login as user1, list other users' volumes
    UserGroupInformation.setLoginUser(user1);
    checkUser(user2, Arrays.asList(VOLUME_2, VOLUME_3, VOLUME_4,
        VOLUME_5), true, false);

    // Add "s3v" created default by OM.
    checkUser(AclTests.ADMIN_UGI, Arrays.asList(VOLUME_1, VOLUME_2, VOLUME_3,
        VOLUME_4, VOLUME_5, "s3v"), true);

    UserGroupInformation.setLoginUser(user2);
    checkUser(user1, Arrays.asList(VOLUME_1, VOLUME_3, VOLUME_4,
        VOLUME_5), true);
    checkUser(AclTests.ADMIN_UGI, Arrays.asList(VOLUME_1, VOLUME_2, VOLUME_3,
        VOLUME_4, VOLUME_5, "s3v"), true);

    // list volumes should success for user with shortname different from
    // full name.
    UserGroupInformation.setLoginUser(user3);
    checkUser(user3, Collections.singletonList(VOLUME_5), true, true);
  }

  /**
   * Check if listVolume of other users than the login user works as expected.
   * ozone.om.volume.listall.allowed = false
   * Only admin should be able to list other users' volumes with this config.
   */
  @Test
  public void testListVolumeWithOtherUsersListAllDisallowed() throws Exception {
    // ozone.acl.enabled = true, ozone.om.volume.listall.allowed = false
    assumeAclEnabled(true);
    setListAllVolumesAllowed(false);

    // Login as user1, list other users' volumes, expect failure
    UserGroupInformation.setLoginUser(user1);
    checkUser(user2, Arrays.asList(VOLUME_2, VOLUME_3, VOLUME_4,
        VOLUME_5), false, false);
    // Add "s3v" created default by OM.
    checkUser(AclTests.ADMIN_UGI, Arrays.asList(VOLUME_1, VOLUME_2, VOLUME_3,
        VOLUME_4, VOLUME_5, "s3v"), false, false);

    // While admin should be able to list volumes just fine.
    UserGroupInformation.setLoginUser(AclTests.ADMIN_UGI);
    checkUser(user1, Arrays.asList(VOLUME_1, VOLUME_3, VOLUME_4,
        VOLUME_5), true);
    checkUser(user2, Arrays.asList(VOLUME_2, VOLUME_3, VOLUME_4,
        VOLUME_5), true);
  }

  @Test
  public void testAclEnabledListAllAllowed() throws Exception {
    // ozone.acl.enabled = true, ozone.om.volume.listall.allowed = true
    assumeAclEnabled(true);
    setListAllVolumesAllowed(true);

    checkUser(user1, Arrays.asList(VOLUME_1, VOLUME_3, VOLUME_4,
        VOLUME_5), true);
    checkUser(user2, Arrays.asList(VOLUME_2, VOLUME_3, VOLUME_4,
        VOLUME_5), true);

    // Add "s3v" created default by OM.
    checkUser(AclTests.ADMIN_UGI, Arrays.asList(VOLUME_1, VOLUME_2, VOLUME_3,
        VOLUME_4, VOLUME_5, "s3v"), true);
  }

  @Test
  public void testAclEnabledListAllDisallowed() throws Exception {
    // ozone.acl.enabled = true, ozone.om.volume.listall.allowed = false
    assumeAclEnabled(true);
    setListAllVolumesAllowed(false);

    // The default user is AclTests.ADMIN_UGI as set in init(),
    // listall always succeeds if we use that UGI, we should use non-admin here
    UserGroupInformation.setLoginUser(user1);
    checkUser(user1, Arrays.asList(VOLUME_1, VOLUME_3, VOLUME_4,
        VOLUME_5), false);
    UserGroupInformation.setLoginUser(user2);
    checkUser(user2, Arrays.asList(VOLUME_2, VOLUME_3, VOLUME_4,
        VOLUME_5), false);
    UserGroupInformation.setLoginUser(AclTests.ADMIN_UGI);
    // Add "s3v" created default by OM.
    checkUser(AclTests.ADMIN_UGI, Arrays.asList(VOLUME_1, VOLUME_2,
        VOLUME_3, VOLUME_4, VOLUME_5, "s3v"), true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAclDisabledAdmin(boolean allowListAllVolumes) throws Exception {
    // ozone.acl.enabled = false, ozone.om.volume.listall.allowed = don't care
    assumeAclEnabled(false);
    setListAllVolumesAllowed(allowListAllVolumes);

    checkUser(user1, Arrays.asList(VOLUME_1, VOLUME_3, VOLUME_5),
        true);
    checkUser(user2, Arrays.asList(VOLUME_2, VOLUME_4),
        true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAclDisabled(boolean allowListAllVolumes) throws Exception {
    // ozone.acl.enabled = false, ozone.om.volume.listall.allowed = don't care
    assumeAclEnabled(false);
    setListAllVolumesAllowed(allowListAllVolumes);

    // If ACL is disabled, all permission checks are disabled in Ozone by design
    UserGroupInformation.setLoginUser(user1);
    checkUser(user1, Arrays.asList(VOLUME_1, VOLUME_3, VOLUME_5),
        true);
    UserGroupInformation.setLoginUser(user2);
    checkUser(user2, Arrays.asList(VOLUME_2, VOLUME_4),
        true);  // listall will succeed since acl is disabled
  }
}

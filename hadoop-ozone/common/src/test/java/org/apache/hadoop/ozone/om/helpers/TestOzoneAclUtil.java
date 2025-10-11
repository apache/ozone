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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.hdds.conf.OzoneConfiguration.newInstanceOf;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/**
 * Test for OzoneAcls utility class.
 */
public class TestOzoneAclUtil {

  private static final List<OzoneAcl> DEFAULT_ACLS =
      getDefaultAcls();

  private static final OzoneAcl USER1 = OzoneAcl.of(USER, "user1",
      ACCESS, ACLType.READ_ACL);

  private static final OzoneAcl GROUP1 = OzoneAcl.of(GROUP, "group1",
      ACCESS, ACLType.ALL);

  @Test
  public void testAddAcl() throws IOException {
    List<OzoneAcl> currentAcls = getDefaultAcls();
    assertFalse(currentAcls.isEmpty());

    // Add new permission to existing acl entry.
    OzoneAcl oldAcl = currentAcls.get(0);
    OzoneAcl newAcl = OzoneAcl.of(oldAcl.getType(), oldAcl.getName(),
        ACCESS, ACLType.READ_ACL);

    addAndVerifyAcl(currentAcls, newAcl, true, DEFAULT_ACLS.size());
    // Add same permission again and verify result
    addAndVerifyAcl(currentAcls, newAcl, false, DEFAULT_ACLS.size());

    // Add a new user acl entry.
    addAndVerifyAcl(currentAcls, USER1, true, DEFAULT_ACLS.size() + 1);
    // Add same acl entry again and verify result
    addAndVerifyAcl(currentAcls, USER1, false, DEFAULT_ACLS.size() + 1);

    // Add a new group acl entry.
    addAndVerifyAcl(currentAcls, GROUP1, true, DEFAULT_ACLS.size() + 2);
    // Add same acl entry again and verify result
    addAndVerifyAcl(currentAcls, GROUP1, false, DEFAULT_ACLS.size() + 2);
  }

  @Test
  public void testRemoveAcl() {
    List<OzoneAcl> currentAcls = null;

    // add/remove to/from null OzoneAcls
    removeAndVerifyAcl(currentAcls, USER1, false, 0);
    addAndVerifyAcl(currentAcls, USER1, false, 0);
    removeAndVerifyAcl(currentAcls, USER1, false, 0);

    currentAcls = getDefaultAcls();
    assertFalse(currentAcls.isEmpty());

    // Add new permission to existing acl entru.
    OzoneAcl oldAcl = currentAcls.get(0);
    OzoneAcl newAcl = OzoneAcl.of(oldAcl.getType(), oldAcl.getName(),
        ACCESS, ACLType.READ_ACL);

    // Remove non existing acl entry
    removeAndVerifyAcl(currentAcls, USER1, false, DEFAULT_ACLS.size());

    // Remove non existing acl permission
    removeAndVerifyAcl(currentAcls, newAcl, false, DEFAULT_ACLS.size());

    // Add new permission to existing acl entry.
    addAndVerifyAcl(currentAcls, newAcl, true, DEFAULT_ACLS.size());

    // Remove the new permission added.
    removeAndVerifyAcl(currentAcls, newAcl, true, DEFAULT_ACLS.size());

    removeAndVerifyAcl(currentAcls, oldAcl, true, DEFAULT_ACLS.size() - 1);
  }

  private void addAndVerifyAcl(List<OzoneAcl> currentAcls, OzoneAcl addedAcl,
                               boolean expectedResult, int expectedSize) {
    assertEquals(expectedResult, OzoneAclUtil.addAcl(currentAcls, addedAcl));
    if (currentAcls != null) {
      boolean verified = verifyAclAdded(currentAcls, addedAcl);
      assertTrue(verified, "addedAcl: " + addedAcl + " should exist in the" +
          " current acls: " + currentAcls);
      assertEquals(expectedSize, currentAcls.size());
    }
  }

  private void removeAndVerifyAcl(List<OzoneAcl> currentAcls,
                                  OzoneAcl removedAcl, boolean expectedResult,
                                  int expectedSize) {
    assertEquals(expectedResult, OzoneAclUtil.removeAcl(currentAcls,
        removedAcl));
    if (currentAcls != null) {
      boolean verified = verifyAclRemoved(currentAcls, removedAcl);
      assertTrue(verified, "removedAcl: " + removedAcl + " should not exist " +
          "in the current acls: " + currentAcls);
      assertEquals(expectedSize, currentAcls.size());
    }
  }

  private boolean verifyAclRemoved(List<OzoneAcl> acls, OzoneAcl removedAcl) {
    for (OzoneAcl acl : acls) {
      if (acl.getName().equals(removedAcl.getName()) &&
          acl.getType().equals(removedAcl.getType()) &&
          acl.getAclScope().equals(removedAcl.getAclScope())) {
        for (ACLType t : removedAcl.getAclList()) {
          if (acl.isSet(t)) {
            return false;
          }
        }
        return true;
      }
    }
    return true;
  }

  private boolean verifyAclAdded(List<OzoneAcl> acls, OzoneAcl newAcl) {
    for (OzoneAcl acl : acls) {
      if (acl.getName().equals(newAcl.getName()) &&
          acl.getType().equals(newAcl.getType()) &&
          acl.getAclScope().equals(newAcl.getAclScope())) {
        for (ACLType t : newAcl.getAclList()) {
          if (!acl.isSet(t)) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  /**
   * Helper function to get default acl list for current user.
   *
   * @return list of ozoneAcls.
   * @throws IOException
   * */
  private static List<OzoneAcl> getDefaultAcls() {
    List<OzoneAcl> ozoneAcls = new ArrayList<>();
    //User ACL
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      ugi = UserGroupInformation.createRemoteUser("user0");
    }

    OmConfig omConfig = newInstanceOf(OmConfig.class);

    OzoneAclUtil.addAcl(ozoneAcls, OzoneAcl.of(USER,
        ugi.getUserName(), ACCESS, omConfig.getUserDefaultRights()));
    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(ugi.getGroupNames());
    userGroups.stream().forEach((group) -> OzoneAclUtil.addAcl(ozoneAcls,
        OzoneAcl.of(GROUP, group, ACCESS, omConfig.getGroupDefaultRights())));
    return ozoneAcls;
  }

  // Ported from tests added with HDDS-4606 for OmOzoneAclMap
  @Test
  public void testAddDefaultAcl() {
    List<OzoneAcl> ozoneAcls = new ArrayList<>();
    OzoneAclUtil.addAcl(ozoneAcls,
        OzoneAcl.parseAcl("user:masstter:rx[DEFAULT]"));
    OzoneAclUtil.addAcl(ozoneAcls,
        OzoneAcl.parseAcl("user:masstter:rw[DEFAULT]"));

    //[user:masstter:rwx[DEFAULT]]
    assertEquals(1, ozoneAcls.size());
    assertEquals(DEFAULT, ozoneAcls.get(0).getAclScope());

    ozoneAcls = new ArrayList<>();
    OzoneAclUtil.addAcl(ozoneAcls,
        OzoneAcl.parseAcl("user:masstter:rx"));
    OzoneAclUtil.addAcl(ozoneAcls,
        OzoneAcl.parseAcl("user:masstter:rw[ACCESS]"));

    //[user:masstter:rwx[ACCESS]]
    assertEquals(1, ozoneAcls.size());
    assertEquals(ACCESS, ozoneAcls.get(0).getAclScope());

    ozoneAcls = new ArrayList<>();
    OzoneAclUtil.addAcl(ozoneAcls,
        OzoneAcl.parseAcl("user:masstter:rwx[DEFAULT]"));
    OzoneAclUtil.addAcl(ozoneAcls,
        OzoneAcl.parseAcl("user:masstter:rwx[ACCESS]"));

    //[user:masstter:rwx[ACCESS], user:masstter:rwx[DEFAULT]]
    assertEquals(2, ozoneAcls.size());
    assertNotEquals(ozoneAcls.get(0).getAclScope(),
        ozoneAcls.get(1).getAclScope());
    assertEquals(ozoneAcls.get(0).getAclByteString(), ozoneAcls.get(1).getAclByteString());
  }
}

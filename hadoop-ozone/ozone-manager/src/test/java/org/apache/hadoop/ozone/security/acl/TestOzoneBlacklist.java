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

package org.apache.hadoop.ozone.security.acl;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.server.OzoneBlacklist;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test Ozone blacklist from OzoneNativeAuthorizer.
 */
public class TestOzoneBlacklist {

  private OzoneNativeAuthorizer nativeAuthorizer;

  @BeforeEach
  public void setup() {
    nativeAuthorizer = new OzoneNativeAuthorizer();
  }

  @Test
  public void testCreateVolume() throws Exception {
    UserGroupInformation.createUserForTesting("testuser",
        new String[]{"testgroup"});
    try {
      OzoneObj obj = getTestVolumeobj("testvolume");
      RequestContext context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.CREATE);
      testBlacklistedUserOperations(obj, context);
      testBlacklistedGroupOperations(obj, context);
    } finally {
      UserGroupInformation.reset();
    }
  }

  @Test
  public void testBucketOperation() throws OMException {
    UserGroupInformation.createUserForTesting("testuser",
        new String[]{"testgroup"});
    try {
      OzoneObj obj = getTestBucketobj("testbucket");
      RequestContext context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.LIST);
      nativeAuthorizer.setOzoneReadBlacklist(new OzoneBlacklist(
          Collections.singletonList("testuser"), null));
      assertFalse(nativeAuthorizer.checkAccess(obj, context),
          "matching read only blacklisted users are not allowed to"
              + " perform read operations");

      context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.READ);
      assertFalse(nativeAuthorizer.checkAccess(obj, context),
          "matching read blacklisted users are not allowed to"
              + " perform read operations");

      context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.READ_ACL);
      assertFalse(nativeAuthorizer.checkAccess(obj, context),
          "matching read blacklisted users are not allowed to"
              + " perform read operations");

      context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.WRITE);
      RequestContext finalContext = context;
      assertThrows(NullPointerException.class,
          () -> nativeAuthorizer.checkAccess(obj, finalContext));

      nativeAuthorizer.setOzoneReadBlacklist(new OzoneBlacklist(
          null, Collections.singletonList("testgroup")));
      context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.READ_ACL);
      assertFalse(nativeAuthorizer.checkAccess(obj, context),
          "matching blacklisted read only users are not allowed to"
              + " perform read operations");
    } finally {
      UserGroupInformation.reset();
    }
  }

  @Test
  public void testListAllVolume() throws Exception {
    UserGroupInformation.createUserForTesting("testuser",
        new String[]{"testgroup"});
    try {
      OzoneObj obj = getTestVolumeobj("/");
      RequestContext context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.LIST);
      testBlacklistedUserOperations(obj, context);
      testBlacklistedGroupOperations(obj, context);
    } finally {
      UserGroupInformation.reset();
    }
  }

  private void testBlacklistedUserOperations(
      OzoneObj obj, RequestContext context) throws OMException {
    nativeAuthorizer.setOzoneAdmins(new OzoneAdmins(
        Collections.singletonList("testuser")));
    assertTrue(nativeAuthorizer.checkAccess(obj, context),
        "admins are allowed to perform any operations");

    nativeAuthorizer.setOzoneBlacklist(
        new OzoneBlacklist(Collections.singletonList("testuser")));
    assertFalse(nativeAuthorizer.checkAccess(obj, context),
        "matching blacklisted users are not allowed to perform"
            + " any operations");

    nativeAuthorizer.setOzoneBlacklist(
        new OzoneBlacklist(asList("testuser2", "testuser")));
    assertFalse(nativeAuthorizer.checkAccess(obj, context),
        "matching blacklisted users are not allowed to perform"
            + " any operations");

    nativeAuthorizer.setOzoneBlacklist(
        new OzoneBlacklist(asList("testuser2", "testuser3")));
    assertTrue(nativeAuthorizer.checkAccess(obj, context),
        "non-blacklisted users should be allowed to perform"
            + " operations");

    nativeAuthorizer.setOzoneReadBlacklist(new OzoneBlacklist(
        Collections.singletonList("testuser"), null));
    if (context.getAclRights() == IAccessAuthorizer.ACLType.LIST
        || context.getAclRights() == IAccessAuthorizer.ACLType.READ
        || context.getAclRights() == IAccessAuthorizer.ACLType.READ_ACL) {
      assertFalse(nativeAuthorizer.checkAccess(obj, context),
          "matching read blacklisted users are not allowed to perform"
              + " read operations");
    } else {
      assertTrue(nativeAuthorizer.checkAccess(obj, context),
          "non-matched read blacklisted users should still be able"
              + " to perform operations");
    }

    nativeAuthorizer.setOzoneReadBlacklist(new OzoneBlacklist(
        Collections.singletonList("testuser1"), null));
    assertTrue(nativeAuthorizer.checkAccess(obj, context),
        "admins are allowed to perform any operations");
  }

  private void testBlacklistedGroupOperations(
      OzoneObj obj, RequestContext context) throws Exception {
    nativeAuthorizer.setOzoneAdmins(new OzoneAdmins(null,
        asList("testgroup", "anothergroup")));
    assertTrue(nativeAuthorizer.checkAccess(obj, context),
        "users in admin groups are allowed to perform any operations");

    nativeAuthorizer.setOzoneBlacklist(
        new OzoneBlacklist(null,
            Collections.singletonList("testgroup")));
    assertFalse(nativeAuthorizer.checkAccess(obj, context),
        "users in matching blacklisted groups are not allowed to"
            + " perform any operations");

    nativeAuthorizer.setOzoneBlacklist(
        new OzoneBlacklist(null,
            asList("testgroup", "anothergroup2")));
    assertFalse(nativeAuthorizer.checkAccess(obj, context),
        "users in matching blacklisted groups are not allowed to"
            + " perform any operations");

    nativeAuthorizer.setOzoneBlacklist(
        new OzoneBlacklist(null,
            asList("testgroup2", "testgroup3")));
    assertTrue(nativeAuthorizer.checkAccess(obj, context),
        "users in admin groups and not in blacklist groups are"
            + " allowed to perform any operations");

    nativeAuthorizer.setOzoneReadBlacklist(new OzoneBlacklist(null,
        Collections.singletonList("testgroup")));
    if (context.getAclRights() == IAccessAuthorizer.ACLType.LIST
        || context.getAclRights() == IAccessAuthorizer.ACLType.READ
        || context.getAclRights() == IAccessAuthorizer.ACLType.READ_ACL) {
      assertFalse(nativeAuthorizer.checkAccess(obj, context),
          "matching read blacklisted users are not allowed to"
              + " perform read operations");
    } else {
      assertTrue(nativeAuthorizer.checkAccess(obj, context),
          "non-matched read blacklisted users should still be"
              + " able to perform operations");
    }

    nativeAuthorizer.setOzoneReadBlacklist(new OzoneBlacklist(null,
        Collections.singletonList("testgroup1")));
    assertTrue(nativeAuthorizer.checkAccess(obj, context),
        "users in admin groups and not in blacklist groups are"
            + " allowed to perform any operations");
  }

  private RequestContext getUserRequestContext(String username,
      IAccessAuthorizer.ACLType type) {
    return RequestContext.newBuilder()
        .setClientUgi(UserGroupInformation.createRemoteUser(username))
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(type)
        .build();
  }

  private OzoneObj getTestVolumeobj(String volumename) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumename).build();
  }

  private OzoneObj getTestBucketobj(String bucketname) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(bucketname).build();
  }
}

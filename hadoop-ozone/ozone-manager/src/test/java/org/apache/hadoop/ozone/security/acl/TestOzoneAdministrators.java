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

package org.apache.hadoop.ozone.security.acl;

import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static java.util.Arrays.asList;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Ozone Administrators from OzoneNativeAuthorizer.
 */
public class TestOzoneAdministrators {

  private static OzoneNativeAuthorizer nativeAuthorizer;

  @BeforeAll
  public static void setup() {
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
      testAdminOperations(obj, context);
      testGroupAdminOperations(obj, context);
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
      nativeAuthorizer.setOzoneReadOnlyAdmins(new OzoneAdmins(
          Collections.singletonList("testuser"), null));
      assertTrue(nativeAuthorizer.checkAccess(obj, context),
          "matching read only admins are allowed to preform" +
          "read operations");

      context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.READ);
      assertTrue(nativeAuthorizer.checkAccess(obj, context),
          "matching read only admins are allowed to preform" +
          "read operations");

      context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.READ_ACL);
      assertTrue(nativeAuthorizer.checkAccess(obj, context),
          "matching read only admins are allowed to preform" +
          "read operations");

      context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.WRITE);
      RequestContext finalContext = context;
      // ACLType is WRITE
      // execute volumeManager.checkAccess volumeManager is null
      assertThrows(NullPointerException.class,
          () -> nativeAuthorizer.checkAccess(obj, finalContext));

      nativeAuthorizer.setOzoneReadOnlyAdmins(new OzoneAdmins(
          null, Collections.singletonList("testgroup")));
      context = getUserRequestContext("testuser",
          IAccessAuthorizer.ACLType.READ_ACL);
      assertTrue(nativeAuthorizer.checkAccess(obj, context),
          "matching read only admins are allowed to preform" +
          "read operations");
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
      testAdminOperations(obj, context);
      testGroupAdminOperations(obj, context);
    } finally {
      UserGroupInformation.reset();
    }
  }

  private void testAdminOperations(OzoneObj obj, RequestContext context)
      throws OMException {
    nativeAuthorizer.setOzoneAdmins(new OzoneAdmins(Collections.emptyList()));
    assertFalse(nativeAuthorizer.checkAccess(obj, context), "empty" +
        " admin list disallow anyone to perform " +
            "admin operations");

    nativeAuthorizer.setOzoneAdmins(new OzoneAdmins(
        Collections.singletonList(OZONE_ADMINISTRATORS_WILDCARD)));
    assertTrue(nativeAuthorizer.checkAccess(obj, context),
        "wildcard admin allows everyone to perform admin" +
        " operations");

    nativeAuthorizer.setOzoneAdmins(new OzoneAdmins(
        Collections.singletonList("testuser")));
    assertTrue(nativeAuthorizer.checkAccess(obj, context),
        "matching admins are allowed to perform admin " +
            "operations");

    nativeAuthorizer.setOzoneAdmins(new OzoneAdmins(
        asList(new String[]{"testuser2", "testuser"})));
    assertTrue(nativeAuthorizer.checkAccess(obj, context),
        "matching admins are allowed to perform admin " +
            "operations");

    nativeAuthorizer.setOzoneAdmins(new OzoneAdmins(
        asList(new String[]{"testuser2", "testuser3"})));
    assertFalse(nativeAuthorizer.checkAccess(obj, context),
        "mismatching admins are not allowed perform " +
        "admin operations");

    nativeAuthorizer.setOzoneReadOnlyAdmins(new OzoneAdmins(
        Collections.singletonList("testuser"), null));
    if (context.getAclRights() == IAccessAuthorizer.ACLType.LIST) {
      assertTrue(nativeAuthorizer.checkAccess(obj, context),
          "matching read only user are allowed to preform" +
          "read operations");
    } else if (context.getAclRights() == IAccessAuthorizer.ACLType.CREATE) {
      assertFalse(nativeAuthorizer.checkAccess(obj, context),
          "mismatching read only user are allowed to preform" +
          "read operations");
    }

    nativeAuthorizer.setOzoneReadOnlyAdmins(new OzoneAdmins(
        Collections.singletonList("testuser1"), null));
    assertFalse(nativeAuthorizer.checkAccess(obj, context),
        "mismatching read only user are allowed to preform" +
        "read operations");
  }

  private void testGroupAdminOperations(OzoneObj obj, RequestContext context)
      throws OMException {
    nativeAuthorizer.setOzoneAdmins(
        new OzoneAdmins(null, asList("testgroup", "anothergroup")));
    assertTrue(nativeAuthorizer.checkAccess(obj, context), "Users " +
            "from matching admin groups " +
        "are allowed to perform admin operations");

    nativeAuthorizer.setOzoneAdmins(
            new OzoneAdmins(null, asList("wronggroup")));
    assertFalse(nativeAuthorizer.checkAccess(obj, context), "Users" +
            " from mismatching admin groups " +
        "are allowed to perform admin operations");

    nativeAuthorizer.setOzoneReadOnlyAdmins(new OzoneAdmins(
        null, Collections.singletonList("testgroup")));
    if (context.getAclRights() == IAccessAuthorizer.ACLType.LIST) {
      assertTrue(nativeAuthorizer.checkAccess(obj, context),
          "matching read only groups are allowed to preform" +
          "read operations");
    } else if (context.getAclRights() == IAccessAuthorizer.ACLType.CREATE) {
      assertFalse(nativeAuthorizer.checkAccess(obj, context),
          "mismatching read only groups are allowed to " +
          "preform read operations");
    }

    nativeAuthorizer.setOzoneReadOnlyAdmins(new OzoneAdmins(
        null, Collections.singletonList("testgroup1")));
    assertFalse(nativeAuthorizer.checkAccess(obj, context),
        "mismatching read only groups are allowed to preform" +
        "read operations");
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

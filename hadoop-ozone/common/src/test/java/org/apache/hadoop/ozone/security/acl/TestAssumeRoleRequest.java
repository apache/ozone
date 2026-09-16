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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AssumeRoleRequest}.
 */
public class TestAssumeRoleRequest {

  @Test
  public void testConstructorAndGetters() {
    final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("om");
    final IOzoneObj bucketObj =
        OzoneObjInfo.Builder.newBuilder()
            .setResType(OzoneObj.ResourceType.BUCKET)
            .setStoreType(OzoneObj.StoreType.OZONE)
            .setVolumeName("s3v")
            .setBucketName("myBucket")
            .build();
    final Set<IOzoneObj> objects = Collections.singleton(bucketObj);
    final Set<IAccessAuthorizer.ACLType> permissions = Collections.singleton(IAccessAuthorizer.ACLType.READ);
    final Set<String> s3Actions = Collections.emptySet();

    final Set<AssumeRoleRequest.OzoneGrant> grants = new HashSet<>();
    grants.add(new AssumeRoleRequest.OzoneGrant(objects, permissions, s3Actions));

    final AssumeRoleRequest assumeRoleRequest1 = new AssumeRoleRequest(
        "host", null, ugi, "roleA", grants);
    final AssumeRoleRequest assumeRoleRequest2 = new AssumeRoleRequest(
        "host", null, ugi, "roleA", grants);

    final AssumeRoleRequest.OzoneGrant grant = grants.iterator().next();
    assertEquals(objects, grant.getObjects());
    assertEquals(permissions, grant.getPermissions());
    assertEquals(s3Actions, grant.getS3Actions());
    // Ensure the s3 actions are not modifiable
    assertThrows(UnsupportedOperationException.class, () -> grant.getS3Actions().add("GetObject"));

    assertEquals("host", assumeRoleRequest1.getHost());
    assertNull(assumeRoleRequest1.getIp());
    assertSame(ugi, assumeRoleRequest1.getClientUgi());
    assertEquals("roleA", assumeRoleRequest1.getTargetRoleName());
    assertEquals(grants, assumeRoleRequest1.getGrants());

    assertEquals(assumeRoleRequest1, assumeRoleRequest2);
    assertEquals(assumeRoleRequest1.hashCode(), assumeRoleRequest2.hashCode());

    final AssumeRoleRequest assumeRoleRequest3 = new AssumeRoleRequest(
        "host", null, ugi, "roleB", null);
    assertNotEquals(assumeRoleRequest1, assumeRoleRequest3);
  }

  @Test
  public void testGrantsWithS3Actions() {
    final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("om");

    final IOzoneObj bucketObj =
        OzoneObjInfo.Builder.newBuilder()
            .setResType(OzoneObj.ResourceType.BUCKET)
            .setStoreType(OzoneObj.StoreType.OZONE)
            .setVolumeName("s3v")
            .setBucketName("myBucket")
            .build();

    final Set<IOzoneObj> objects = Collections.singleton(bucketObj);
    final Set<IAccessAuthorizer.ACLType> permissions = Collections.singleton(IAccessAuthorizer.ACLType.READ);

    final Set<String> s3Actions = new LinkedHashSet<>();
    s3Actions.add("GetObject");
    s3Actions.add("PutObject");

    final AssumeRoleRequest.OzoneGrant grantWithActions = new AssumeRoleRequest.OzoneGrant(
        objects, permissions, s3Actions);
    final AssumeRoleRequest.OzoneGrant grantWithoutActions = new AssumeRoleRequest.OzoneGrant(
        objects, permissions, Collections.emptySet());

    assertEquals(objects, grantWithActions.getObjects());
    assertEquals(permissions, grantWithActions.getPermissions());
    assertEquals(s3Actions, grantWithActions.getS3Actions());
    assertNotEquals(grantWithActions, grantWithoutActions);
    assertNotEquals(grantWithActions.hashCode(), grantWithoutActions.hashCode());

    final Set<AssumeRoleRequest.OzoneGrant> grantsWithActionsSet = Collections.singleton(grantWithActions);
    final Set<AssumeRoleRequest.OzoneGrant> grantsWithoutActionsSet = Collections.singleton(grantWithoutActions);

    final AssumeRoleRequest requestWithActions = new AssumeRoleRequest(
        "host", null, ugi, "roleA", grantsWithActionsSet);
    final AssumeRoleRequest requestWithoutActions = new AssumeRoleRequest(
        "host", null, ugi, "roleA", grantsWithoutActionsSet);

    assertNotEquals(requestWithActions, requestWithoutActions);
    assertNotEquals(requestWithActions.hashCode(), requestWithoutActions.hashCode());
  }
}



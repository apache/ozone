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

import java.util.Collections;
import java.util.HashSet;
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
    final Set<AssumeRoleRequest.OzoneGrant> grants = new HashSet<>();
    grants.add(
        new AssumeRoleRequest.OzoneGrant(
            Collections.singleton(
                OzoneObjInfo.Builder.newBuilder()
                    .setResType(OzoneObj.ResourceType.BUCKET)
                    .setStoreType(OzoneObj.StoreType.OZONE)
                    .setVolumeName("s3v")
                    .setBucketName("myBucket")
                    .build()),
            Collections.singleton(IAccessAuthorizer.ACLType.READ)));

    final AssumeRoleRequest assumeRoleRequest1 = new AssumeRoleRequest(
        "host", null, ugi, "roleA", grants);
    final AssumeRoleRequest assumeRoleRequest2 = new AssumeRoleRequest(
        "host", null, ugi, "roleA", grants);

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
}



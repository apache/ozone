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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/**
 * Test request context.
 */
public class TestRequestContext {

  @Test
  void testRecursiveAccessFlag() {
    RequestContext.Builder builder = RequestContext.newBuilder();

    assertFalse(builder.build().isRecursiveAccessCheck(), "default value");

    builder.setRecursiveAccessCheck(true);
    assertTrue(builder.build().isRecursiveAccessCheck());

    builder.setRecursiveAccessCheck(false);
    assertFalse(builder.build().isRecursiveAccessCheck());
  }

  @Test
  void testSessionPolicy() {
    RequestContext.Builder builder = RequestContext.newBuilder();
    assertNull(builder.build().getSessionPolicy(), "default value");

    final String policy = "{\"Statement\":[]}";
    builder.setSessionPolicy(policy);
    assertEquals(policy, builder.build().getSessionPolicy());
  }

  @Test
  public void testToBuilderWithNoModifications() {
    // Create a RequestContext with all fields set
    final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("testUser");
    final String host = "testHost";
    final String serviceId = "testServiceId";
    final String ownerName = "testOwner";
    final String sessionPolicy = "{\"Statement\":[{\"Effect\":\"Allow\"}]}";

    final RequestContext original = RequestContext.newBuilder()
        .setHost(host)
        .setClientUgi(ugi)
        .setServiceId(serviceId)
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(IAccessAuthorizer.ACLType.READ)
        .setOwnerName(ownerName)
        .setRecursiveAccessCheck(true)
        .setSessionPolicy(sessionPolicy)
        .build();

    // Use toBuilder to create a new builder
    final RequestContext.Builder builder = original.toBuilder();
    final RequestContext requestCtxFromToBuilder = builder.build();

    // Verify all fields are preserved
    assertEquals(original.getHost(), requestCtxFromToBuilder.getHost(), "Host should be preserved");
    assertNull(original.getIp(), "IP should be preserved");
    assertEquals(original.getClientUgi(), requestCtxFromToBuilder.getClientUgi(), "ClientUgi should be preserved");
    assertEquals(original.getServiceId(), requestCtxFromToBuilder.getServiceId(), "ServiceId should be preserved");
    assertEquals(original.getAclType(), requestCtxFromToBuilder.getAclType(), "AclType should be preserved");
    assertEquals(original.getAclRights(), requestCtxFromToBuilder.getAclRights(), "AclRights should be preserved");
    assertEquals(original.getOwnerName(), requestCtxFromToBuilder.getOwnerName(), "OwnerName should be preserved");
    assertTrue(original.isRecursiveAccessCheck(), "RecursiveAccessCheck should be preserved");
    assertEquals(original.getSessionPolicy(), requestCtxFromToBuilder.getSessionPolicy(),
        "SessionPolicy should be preserved");
  }

  @Test
  public void testToBuilderWithModifications() {
    // Create an original RequestContext
    final UserGroupInformation originalUgi = UserGroupInformation.createRemoteUser("user1");
    final RequestContext original = RequestContext.newBuilder()
        .setHost("host1")
        .setClientUgi(originalUgi)
        .setServiceId("service1")
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(IAccessAuthorizer.ACLType.READ)
        .setOwnerName("owner1")
        .setRecursiveAccessCheck(false)
        .build();

    // Use toBuilder and modify some fields
    final UserGroupInformation newUgi = UserGroupInformation.createRemoteUser("user2");
    final RequestContext modified = original.toBuilder()
        .setHost("host2")
        .setClientUgi(newUgi)
        .setAclRights(IAccessAuthorizer.ACLType.WRITE)
        .setOwnerName("owner2")
        .setRecursiveAccessCheck(true)
        .setSessionPolicy("{\"Statement\":[]}")
        .build();

    // Verify original is unchanged
    assertEquals("host1", original.getHost(), "Original should be unchanged");
    assertEquals(originalUgi, original.getClientUgi(), "Original UGI should be unchanged");
    assertEquals(IAccessAuthorizer.ACLType.READ, original.getAclRights(), "Original ACL rights should be unchanged");
    assertEquals("owner1", original.getOwnerName(), "Original owner name should be unchanged");
    assertFalse(original.isRecursiveAccessCheck(), "Original recursive flag should be unchanged");
    assertNull(original.getSessionPolicy(), "Original session policy should be unchanged");

    // Verify modified has new values
    assertEquals("host2", modified.getHost(), "Modified host should be updated");
    assertEquals(newUgi, modified.getClientUgi(), "Modified UGI should be updated");
    assertEquals(IAccessAuthorizer.ACLType.WRITE, modified.getAclRights(), "Modified ACL rights should be updated");
    assertEquals("owner2", modified.getOwnerName(), "Modified owner should be updated");
    assertTrue(modified.isRecursiveAccessCheck(), "Modified recursive flag should be updated");
    assertEquals("{\"Statement\":[]}", modified.getSessionPolicy(), "Modified session policy should be updated");
  }
}

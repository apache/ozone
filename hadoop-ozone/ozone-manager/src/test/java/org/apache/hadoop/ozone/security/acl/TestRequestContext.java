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

import java.io.IOException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/**
 * Test request context.
 */
public class TestRequestContext {

  @Test
  public void testRecursiveAccessFlag() throws IOException {
    RequestContext context = getUserRequestContext("om",
            IAccessAuthorizer.ACLType.CREATE, false, "volume1",
            true);
    assertTrue(context.isRecursiveAccessCheck(),
        "Wrongly sets recursiveAccessCheck flag value");

    context = getUserRequestContext("om",
            IAccessAuthorizer.ACLType.CREATE, false, "volume1",
            false);
    assertFalse(context.isRecursiveAccessCheck(),
        "Wrongly sets recursiveAccessCheck flag value");

    context = getUserRequestContext(
            "user1", IAccessAuthorizer.ACLType.CREATE,
            true, "volume1");
    assertFalse(context.isRecursiveAccessCheck(),
        "Wrongly sets recursiveAccessCheck flag value");

    RequestContext.Builder builder = new RequestContext.Builder();

    assertFalse(builder.build().isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");

    builder.setRecursiveAccessCheck(true);
    assertTrue(builder.build().isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");

    context = new RequestContext("host", null,
            null, "serviceId",
            IAccessAuthorizer.ACLIdentityType.GROUP,
            IAccessAuthorizer.ACLType.CREATE, "owner");
    assertFalse(context.isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");

    context = new RequestContext("host", null,
            null, "serviceId",
            IAccessAuthorizer.ACLIdentityType.GROUP,
            IAccessAuthorizer.ACLType.CREATE, "owner", false);
    assertFalse(context.isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");

    context = new RequestContext("host", null,
            null, "serviceId",
            IAccessAuthorizer.ACLIdentityType.GROUP,
            IAccessAuthorizer.ACLType.CREATE, "owner", true);
    assertTrue(context.isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");
  }

  @Test
  public void testSessionPolicy() {
    final RequestContext.Builder builder = new RequestContext.Builder();
    RequestContext context = builder.build();
    assertNull(context.getSessionPolicy(), "sessionPolicy should default to null");

    final String policy = "{\"Statement\":[]}";
    context = new RequestContext.Builder()
        .setSessionPolicy(policy)
        .build();
    assertEquals(policy, context.getSessionPolicy(), "sessionPolicy should be set via builder");

    context = new RequestContext(
        "host", null, null, "serviceId", IAccessAuthorizer.ACLIdentityType.GROUP,
        IAccessAuthorizer.ACLType.CREATE, "owner", true, policy);
    assertTrue(context.isRecursiveAccessCheck(), "recursiveAccessCheck should be true");
    assertEquals(policy, context.getSessionPolicy(), "sessionPolicy should be set via constructor");

    context = RequestContext.getBuilder(
        UserGroupInformation.createRemoteUser("user1"), null, null,
        IAccessAuthorizer.ACLType.CREATE, "volume1", true)
        .setSessionPolicy(policy)
        .build();
    assertEquals(policy, context.getSessionPolicy(), "sessionPolicy should be set via getBuilder + builder");

    context = RequestContext.getBuilder(
        UserGroupInformation.createRemoteUser("user1"), null, null,
        IAccessAuthorizer.ACLType.CREATE, "volume1", true, policy)
        .build();
    assertEquals(
        policy, context.getSessionPolicy(),
        "sessionPolicy should be set via getBuilder (all params) + builder");
  }

  private RequestContext getUserRequestContext(String username,
      IAccessAuthorizer.ACLType type, boolean isOwner, String ownerName,
      boolean recursiveAccessCheck) throws IOException {

    return RequestContext.getBuilder(
            UserGroupInformation.createRemoteUser(username), null, null,
            type, ownerName, recursiveAccessCheck).build();
  }

  private RequestContext getUserRequestContext(String username,
      IAccessAuthorizer.ACLType type, boolean isOwner, String ownerName) {
    return RequestContext.getBuilder(
            UserGroupInformation.createRemoteUser(username), null, null,
            type, ownerName).build();
  }

  @Test
  public void testToBuilderWithNoModifications() {
    // Create a RequestContext with all fields set
    final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("testUser");
    final String host = "testHost";
    final String serviceId = "testServiceId";
    final String ownerName = "testOwner";
    final String sessionPolicy = "{\"Statement\":[{\"Effect\":\"Allow\"}]}";

    final RequestContext original = new RequestContext(
        host, null, ugi, serviceId, IAccessAuthorizer.ACLIdentityType.USER, IAccessAuthorizer.ACLType.READ, ownerName,
        true, sessionPolicy);

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
    final RequestContext original = new RequestContext(
        "host1", null, originalUgi, "service1", IAccessAuthorizer.ACLIdentityType.USER, IAccessAuthorizer.ACLType.READ,
        "owner1", false, null);

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

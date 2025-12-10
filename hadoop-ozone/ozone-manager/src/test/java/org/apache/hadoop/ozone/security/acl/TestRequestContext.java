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

    RequestContext.Builder builder = RequestContext.newBuilder();

    assertFalse(builder.build().isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");

    builder.setRecursiveAccessCheck(true);
    assertTrue(builder.build().isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");

    context = baseBuilder().build();
    assertFalse(context.isRecursiveAccessCheck());

    context = baseBuilder().setRecursiveAccessCheck(false).build();
    assertFalse(context.isRecursiveAccessCheck());

    context = baseBuilder().setRecursiveAccessCheck(true).build();
    assertTrue(context.isRecursiveAccessCheck());
  }

  @Test
  public void testSessionPolicy() {
    RequestContext context = RequestContext.newBuilder()
        .build();
    assertNull(context.getSessionPolicy(), "sessionPolicy should default to null");

    final String policy = "{\"Statement\":[]}";
    context = RequestContext.newBuilder()
        .setSessionPolicy(policy)
        .build();
    assertEquals(policy, context.getSessionPolicy(), "sessionPolicy should be set via builder");

    context = RequestContext.newBuilder()
        .setHost("host")
        .setIp(null)
        .setClientUgi(null)
        .setServiceId("serviceId")
        .setAclType(IAccessAuthorizer.ACLIdentityType.GROUP)
        .setAclRights(IAccessAuthorizer.ACLType.CREATE)
        .setOwnerName("owner")
        .setRecursiveAccessCheck(true)
        .setSessionPolicy(policy)
        .build();
    assertTrue(context.isRecursiveAccessCheck(), "recursiveAccessCheck should be true");
    assertEquals(policy, context.getSessionPolicy(), "sessionPolicy should be set via constructor");

    context = RequestContext.newBuilder()
        .setClientUgi(UserGroupInformation.createRemoteUser("user1"))
        .setIp(null)
        .setHost(null)
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(IAccessAuthorizer.ACLType.CREATE)
        .setOwnerName("volume1")
        .setRecursiveAccessCheck(true)
        .setSessionPolicy(policy)
        .build();
    assertEquals(policy, context.getSessionPolicy(), "sessionPolicy should be set via getBuilder + builder");

    context = RequestContext.newBuilder()
        .setClientUgi(UserGroupInformation.createRemoteUser("user1"))
        .setIp(null)
        .setHost(null)
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(IAccessAuthorizer.ACLType.CREATE)
        .setOwnerName("volume1")
        .setRecursiveAccessCheck(true)
        .setSessionPolicy(policy)
        .build();
    assertEquals(
        policy, context.getSessionPolicy(),
        "sessionPolicy should be set via getBuilder (all params) + builder");
  }

  private RequestContext getUserRequestContext(String username,
      IAccessAuthorizer.ACLType type, boolean isOwner, String ownerName,
      boolean recursiveAccessCheck) throws IOException {

    return RequestContext.newBuilder()
        .setClientUgi(UserGroupInformation.createRemoteUser(username))
        .setIp(null)
        .setHost(null)
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(type)
        .setOwnerName(ownerName)
        .setRecursiveAccessCheck(recursiveAccessCheck)
        .build();
  }

  private RequestContext getUserRequestContext(String username,
      IAccessAuthorizer.ACLType type, boolean isOwner, String ownerName) {

    return RequestContext.newBuilder()
        .setClientUgi(UserGroupInformation.createRemoteUser(username))
        .setIp(null)
        .setHost(null)
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(type)
        .setOwnerName(ownerName)
        .build();
  }

  private RequestContext.Builder baseBuilder() {
    return RequestContext.newBuilder()
        .setHost("host")
        .setIp(null)
        .setClientUgi(null)
        .setServiceId("serviceId")
        .setAclType(IAccessAuthorizer.ACLIdentityType.GROUP)
        .setAclRights(IAccessAuthorizer.ACLType.CREATE)
        .setOwnerName("owner");
  }
}

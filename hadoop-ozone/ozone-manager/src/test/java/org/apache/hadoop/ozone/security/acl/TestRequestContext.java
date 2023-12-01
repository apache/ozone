/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.acl;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Test request context.
 */
public class TestRequestContext {

  @Test
  public void testRecursiveAccessFlag() throws IOException {
    RequestContext context = getUserRequestContext("om",
            IAccessAuthorizer.ACLType.CREATE, false, "volume1",
            true);
    Assertions.assertTrue(context.isRecursiveAccessCheck(),
        "Wrongly sets recursiveAccessCheck flag value");

    context = getUserRequestContext("om",
            IAccessAuthorizer.ACLType.CREATE, false, "volume1",
            false);
    Assertions.assertFalse(context.isRecursiveAccessCheck(),
        "Wrongly sets recursiveAccessCheck flag value");

    context = getUserRequestContext(
            "user1", IAccessAuthorizer.ACLType.CREATE,
            true, "volume1");
    Assertions.assertFalse(context.isRecursiveAccessCheck(),
        "Wrongly sets recursiveAccessCheck flag value");

    RequestContext.Builder builder = new RequestContext.Builder();

    Assertions.assertFalse(builder.build().isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");

    builder.setRecursiveAccessCheck(true);
    Assertions.assertTrue(builder.build().isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");

    context = new RequestContext("host", null,
            null, "serviceId",
            IAccessAuthorizer.ACLIdentityType.GROUP,
            IAccessAuthorizer.ACLType.CREATE, "owner");
    Assertions.assertFalse(context.isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");

    context = new RequestContext("host", null,
            null, "serviceId",
            IAccessAuthorizer.ACLIdentityType.GROUP,
            IAccessAuthorizer.ACLType.CREATE, "owner", false);
    Assertions.assertFalse(context.isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");

    context = new RequestContext("host", null,
            null, "serviceId",
            IAccessAuthorizer.ACLIdentityType.GROUP,
            IAccessAuthorizer.ACLType.CREATE, "owner", true);
    Assertions.assertTrue(context.isRecursiveAccessCheck(),
        "Wrongly sets recursive flag value");
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
}


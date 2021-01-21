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
import org.junit.Assert;
import org.junit.Test;

/**
 * Test request context.
 */
public class TestRequestContext {

  @Test
  public void testRecursiveAccessFlag() {
    RequestContext context = getUserRequestContext("om",
            IAccessAuthorizer.ACLType.CREATE, false, "volume1",
            true);
    Assert.assertTrue("Wrongly sets recursiveAccessCheck flag value",
            context.isRecursiveAccessCheck());

    context = getUserRequestContext("om",
            IAccessAuthorizer.ACLType.CREATE, false, "volume1",
            false);
    Assert.assertFalse("Wrongly sets recursiveAccessCheck flag value",
            context.isRecursiveAccessCheck());

    context = getUserRequestContext(
            "user1", IAccessAuthorizer.ACLType.CREATE,
            true, "volume1");
    Assert.assertFalse("Wrongly sets recursiveAccessCheck flag value",
            context.isRecursiveAccessCheck());

    RequestContext.Builder builder = new RequestContext.Builder();

    Assert.assertFalse("Wrongly sets recursive flag value",
            builder.build().isRecursiveAccessCheck());

    builder.setRecursiveAccessCheck(true);
    Assert.assertTrue("Wrongly sets recursive flag value",
            builder.build().isRecursiveAccessCheck());

    context = new RequestContext("host", null,
            null, "serviceId",
            IAccessAuthorizer.ACLIdentityType.GROUP,
            IAccessAuthorizer.ACLType.CREATE, "owner");
    Assert.assertFalse("Wrongly sets recursive flag value",
            context.isRecursiveAccessCheck());

    context = new RequestContext("host", null,
            null, "serviceId",
            IAccessAuthorizer.ACLIdentityType.GROUP,
            IAccessAuthorizer.ACLType.CREATE, "owner", false);
    Assert.assertFalse("Wrongly sets recursive flag value",
            context.isRecursiveAccessCheck());

    context = new RequestContext("host", null,
            null, "serviceId",
            IAccessAuthorizer.ACLIdentityType.GROUP,
            IAccessAuthorizer.ACLType.CREATE, "owner", true);
    Assert.assertTrue("Wrongly sets recursive flag value",
            context.isRecursiveAccessCheck());
  }

  private RequestContext getUserRequestContext(String username,
      IAccessAuthorizer.ACLType type, boolean isOwner, String ownerName,
      boolean recursiveAccessCheck) {
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


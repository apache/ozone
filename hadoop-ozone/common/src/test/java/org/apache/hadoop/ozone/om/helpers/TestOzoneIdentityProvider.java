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
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

/**
 * Test class for {@link OzoneIdentityProvider}.
 */
public class TestOzoneIdentityProvider {
  private static OzoneIdentityProvider identityProvider;
  private static final String ACCESS_ID = "testuser";

  private static final Schedulable DEFAULT_SCHEDULABLE = new Schedulable() {
    @Override
    public UserGroupInformation getUserGroupInformation() {
      return UserGroupInformation.createRemoteUser(ACCESS_ID);
    }

    @Override
    public int getPriorityLevel() {
      return 0;
    }
  };

  @BeforeAll
  public static void setUp() {
    identityProvider = new OzoneIdentityProvider();
  }

  @Test
  public void testGetUserFromCallerContext() {
    Schedulable callerContextSchedulable = Mockito.mock(Schedulable.class);

    CallerContext callerContext =
        new CallerContext.Builder(
            ACCESS_ID).build();

    when(callerContextSchedulable.getCallerContext())
        .thenReturn(callerContext);

    String identity = identityProvider.makeIdentity(callerContextSchedulable);
    String usernameFromContext = callerContext.getContext();

    Assertions.assertEquals(usernameFromContext, identity);
  }

  @Test
  public void testGetUserFromUGI() {
    String identity = identityProvider.makeIdentity(DEFAULT_SCHEDULABLE);

    // defaultSchedulable doesn't override CallerContext and
    // accessing it should throw an exception.
    UnsupportedOperationException uoex = Assertions
        .assertThrows(UnsupportedOperationException.class,
            DEFAULT_SCHEDULABLE::getCallerContext);
    Assertions.assertEquals("Invalid operation.",
        uoex.getMessage());

    String usernameFromUGI = DEFAULT_SCHEDULABLE
        .getUserGroupInformation().getShortUserName();
    Assertions.assertEquals(usernameFromUGI, identity);
  }
}

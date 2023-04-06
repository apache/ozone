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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

/**
 * Test class for {@link OzoneIdentityProvider}
 */
public class TestOzoneIdentityProvider {
  private static OzoneIdentityProvider identityProvider;
  private static Schedulable schedulable;
  private static final String ACCESS_ID = "testuser";

  @BeforeAll
  public static void setUp() {
    identityProvider = new OzoneIdentityProvider();
    schedulable = Mockito.mock(Schedulable.class);
  }

  @AfterEach
  public void reset() {
    when(schedulable.getUserGroupInformation()).thenReturn(null);
    when(schedulable.getCallerContext()).thenReturn(null);
  }

  @Test
  public void testGetUserFromCallerContext() {
    CallerContext callerContext =
        new CallerContext.Builder(
            ACCESS_ID).build();

    when(schedulable.getCallerContext()).thenReturn(callerContext);

    String identity = identityProvider.makeIdentity(schedulable);
    String username = callerContext.getContext();

    Assertions.assertEquals(username, identity);
  }

  @Test
  public void testGetUserFromUGI() {
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(ACCESS_ID);

    when(schedulable.getUserGroupInformation()).thenReturn(ugi);

    String identity = identityProvider.makeIdentity(schedulable);

    // CallerContext doesn't have a value and should be null
    Assertions.assertNull(schedulable.getCallerContext());

    String username = ugi.getShortUserName();
    Assertions.assertEquals(username, identity);
  }
}

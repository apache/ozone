/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for OzoneIdentityProvider.
 */
public class TestOzoneIdentityProvider {

  private static OzoneIdentityProvider identityProvider;
  private static Schedulable schedulable;

  @BeforeAll
  public static void setUp() {
    identityProvider = new OzoneIdentityProvider();
    schedulable = Mockito.mock(Schedulable.class);
  }

  @Test
  public void testGetUserFromUgi() {
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("testUser");
    UserGroupInformation.setLoginUser(ugi);

    String identity = identityProvider.makeIdentity(schedulable);

    String username = ugi.getShortUserName();
    Assertions.assertEquals(username, identity);
  }
}

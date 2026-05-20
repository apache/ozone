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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_CALLER_CONTEXT_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.ipc_.CallerContext;
import org.apache.hadoop.ipc_.Schedulable;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link OzoneIdentityProvider}.
 */
public class TestOzoneIdentityProvider {
  private static OzoneIdentityProvider identityProvider;
  private static final String ACCESS_ID = "testuser";

  /**
   * Schedulable that doesn't override getCallerContext().
   */
  private static final Schedulable DEFAULT_SCHEDULABLE =
      new Schedulable() {
        @Override
        public UserGroupInformation getUserGroupInformation() {
          return UserGroupInformation.createRemoteUser(ACCESS_ID);
        }

        @Override
        public int getPriorityLevel() {
          return 0;
        }
      };

  /**
   * Schedulable that overrides getCallerContext().
   */
  private static final Schedulable CALLER_CONTEXT_SCHEDULABLE =
      new Schedulable() {
        @Override
        public UserGroupInformation getUserGroupInformation() {
          return UserGroupInformation.createRemoteUser("s3g");
        }

        @Override
        public CallerContext getCallerContext() {
          return new CallerContext
              .Builder(OM_S3_CALLER_CONTEXT_PREFIX + ACCESS_ID)
              .build();
        }

        @Override
        public int getPriorityLevel() {
          return 0;
        }
      };

  /**
   * Schedulable that overrides getCallerContext() but its value
   * is set by the user and doesn't have the proper format.
   */
  private static final Schedulable NO_PREFIX_CALLER_CONTEXT_SCHEDULABLE =
      new Schedulable() {
        @Override
        public UserGroupInformation getUserGroupInformation() {
          return UserGroupInformation.createRemoteUser("s3g");
        }

        @Override
        public CallerContext getCallerContext() {
          return new CallerContext.Builder(ACCESS_ID).build();
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
    String identity = identityProvider
        .makeIdentity(CALLER_CONTEXT_SCHEDULABLE);

    assertEquals(ACCESS_ID, identity);
  }

  /**
   * If CallerContext is not prefixed with "S3Auth:S3G|",
   * then it will be ignored and UGI will be used instead.
   */
  @Test
  public void testGetUserWithCallerContextNotSetProperly() {
    String identity = identityProvider
        .makeIdentity(NO_PREFIX_CALLER_CONTEXT_SCHEDULABLE);

    assertFalse(
        NO_PREFIX_CALLER_CONTEXT_SCHEDULABLE
            .getCallerContext().getContext()
            .startsWith(OM_S3_CALLER_CONTEXT_PREFIX));
    assertEquals(
        NO_PREFIX_CALLER_CONTEXT_SCHEDULABLE
            .getUserGroupInformation()
            .getShortUserName(), identity);
  }

  @Test
  public void testGetUserFromUGI() {
    String identity = identityProvider.makeIdentity(DEFAULT_SCHEDULABLE);

    // DEFAULT_SCHEDULABLE doesn't override CallerContext and
    // accessing it should throw an exception.
    UnsupportedOperationException uoex = assertThrows(UnsupportedOperationException.class,
            DEFAULT_SCHEDULABLE::getCallerContext);
    assertEquals("Invalid operation.",
        uoex.getMessage());

    String usernameFromUGI = DEFAULT_SCHEDULABLE
        .getUserGroupInformation().getShortUserName();
    assertEquals(usernameFromUGI, identity);
  }
}

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

package org.apache.hadoop.ozone.om.request.s3.security;

import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS;
import static org.apache.hadoop.security.UserGroupInformation.createRemoteUser;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.ipc_.ExternalCall;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestS3SecretRequestHelper {
  private static final String TEST_ACCESS_ID = "access/server@EXAMPLE.COM";
  private UserGroupInformation testUgi;
  private UserGroupInformation expectedUgi;

  @BeforeEach
  void setUp() {
    KerberosName.setRules(
            "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
                    "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
                    "DEFAULT");
    testUgi = createRemoteUser(TEST_ACCESS_ID, KERBEROS);
    expectedUgi = createRemoteUser(TEST_ACCESS_ID, KERBEROS);
  }

  @AfterEach
  void tearDown() {
    Server.getCurCall().set(null);
  }

  @Test
  void testGettingUgiFromCall() {
    Server.getCurCall().set(new StubCall(testUgi));
    compareUgi(expectedUgi,
               S3SecretRequestHelper.getOrCreateUgi(TEST_ACCESS_ID));
  }

  @Test
  void testGettingUgiFromAccessId() {
    compareUgi(expectedUgi,
               S3SecretRequestHelper.getOrCreateUgi(TEST_ACCESS_ID));
  }

  @Test
  void testGettingUgiWithNoAccessId() {
    assertNull(S3SecretRequestHelper.getOrCreateUgi(null));
  }

  private static void compareUgi(UserGroupInformation expected,
                                 UserGroupInformation actual) {
    assertEquals(expected.getUserName(), actual.getUserName());
    assertEquals(expected.getAuthenticationMethod(),
                 actual.getAuthenticationMethod());
  }

  private static final class StubCall extends ExternalCall<String> {
    private final UserGroupInformation ugi;

    StubCall(UserGroupInformation ugi) {
      super(null);
      this.ugi = ugi;
    }

    @Override
    public UserGroupInformation getRemoteUser() {
      return ugi;
    }
  }
}

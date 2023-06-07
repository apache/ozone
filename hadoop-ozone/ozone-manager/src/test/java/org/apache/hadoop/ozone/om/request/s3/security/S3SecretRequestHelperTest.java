/*
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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.om.request.s3.security;

import org.apache.hadoop.ipc.ExternalCall;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS;
import static org.apache.hadoop.security.UserGroupInformation.createRemoteUser;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class S3SecretRequestHelperTest {
  private static final String TEST_ACCESS_ID = "access/server@realm";
  private static final UserGroupInformation TEST_UGI =
      createRemoteUser(TEST_ACCESS_ID, KERBEROS);

  @Test
  void testGettingUgiFromCall() {
    Server.getCurCall().set(new StubCall(TEST_UGI));
    assertEquals(createRemoteUser(TEST_ACCESS_ID, KERBEROS),
                 S3SecretRequestHelper.getOrCreateUgi(TEST_ACCESS_ID));
  }

  @Test
  void testGettingUgiFromAccessId() {
    assertEquals(createRemoteUser(TEST_ACCESS_ID, KERBEROS),
                 S3SecretRequestHelper.getOrCreateUgi(TEST_ACCESS_ID));
  }

  @Test
  void testGettingUgiWithNoAccessId() {
    assertNull(S3SecretRequestHelper.getOrCreateUgi(null));
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

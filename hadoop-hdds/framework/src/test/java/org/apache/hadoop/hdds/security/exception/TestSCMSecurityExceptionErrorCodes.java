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

package org.apache.hadoop.hdds.security.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Status;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * Test error code mapping between SCM security protocol and exception definitions.
 */
public class TestSCMSecurityExceptionErrorCodes {

  @Test
  public void codeMapping() {
    for (Status status : Status.values()) {
      if (status != Status.GET_ROOT_CA_CERTIFICATE_FAILED
          && status != Status.REVOKE_CERTIFICATE_FAILED) {
        assertEquals(status.name(), ErrorCode.valueOf(status.name()).name());
      }
    }
    assertEquals(ErrorCode.GET_ROOT_CA_CERT_FAILED,
        ErrorCode.valueOf("GET_ROOT_CA_CERT_FAILED"));
    assertThrows(IllegalArgumentException.class,
        () -> ErrorCode.valueOf(Status.REVOKE_CERTIFICATE_FAILED.name()));
  }
}

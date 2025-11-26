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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Test S3 Authentication logic in OzoneManager.
 */
public class TestOzoneManagerS3Auth {

  @AfterEach
  public void tearDown() {
    OzoneManager.setS3Auth(null);
    OzoneManager.setStsTokenIdentifier(null);
  }

  @Test
  public void testGetS3AuthEffectiveAccessIdNoS3Auth() throws Exception {
    OzoneManager.setS3Auth(null);
    assertNull(OzoneManager.getS3AuthEffectiveAccessId());
  }

  @Test
  public void testGetS3AuthEffectiveAccessIdNormal() throws Exception {
    final String accessId = "accessId";
    final S3Authentication s3Auth = S3Authentication.newBuilder()
        .setAccessId(accessId)
        .setSignature("signature")
        .setStringToSign("stringToSign")
        .build();
    OzoneManager.setS3Auth(s3Auth);

    assertEquals(accessId, OzoneManager.getS3AuthEffectiveAccessId());
  }

  @Test
  public void testGetS3AuthEffectiveAccessIdWithSessionToken() throws Exception {
    final String tempAccessId = "ASIA12345";
    final String originalAccessId = "AKIAORIG98765";
    final String sessionToken = "sessionToken";

    final S3Authentication s3Auth = S3Authentication.newBuilder()
        .setAccessId(tempAccessId)
        .setSignature("signature")
        .setStringToSign("stringToSign")
        .setSessionToken(sessionToken)
        .build();
    OzoneManager.setS3Auth(s3Auth);

    final STSTokenIdentifier stsToken = mock(STSTokenIdentifier.class);
    when(stsToken.getOriginalAccessKeyId()).thenReturn(originalAccessId);
    OzoneManager.setStsTokenIdentifier(stsToken);

    assertEquals(originalAccessId, OzoneManager.getS3AuthEffectiveAccessId());
  }

  @Test
  public void testGetS3AuthEffectiveAccessIdWithEmptySessionToken() throws Exception {
    final String accessId = "AKIAORIG98765";
    final String emptySessionToken = "";

    final S3Authentication s3Auth = S3Authentication.newBuilder()
        .setAccessId(accessId)
        .setSignature("sig")
        .setStringToSign("str")
        .setSessionToken(emptySessionToken)
        .build();
    OzoneManager.setS3Auth(s3Auth);

    // Empty session token should be treated as if no session token is present.
    assertEquals(accessId, OzoneManager.getS3AuthEffectiveAccessId());
  }
  
  @Test
  public void testGetS3AuthEffectiveAccessIdWithSessionTokenMissingOriginalAccessKey() {
    final String tempAccessId = "ASIA12345";
    final String sessionToken = "sessionToken";

    final S3Authentication s3Auth = S3Authentication.newBuilder()
        .setAccessId(tempAccessId)
        .setSignature("signature")
        .setStringToSign("stringToSign")
        .setSessionToken(sessionToken)
        .build();
    OzoneManager.setS3Auth(s3Auth);

    final STSTokenIdentifier stsToken = mock(STSTokenIdentifier.class);
    when(stsToken.getOriginalAccessKeyId()).thenReturn(null); // Missing original ID
    OzoneManager.setStsTokenIdentifier(stsToken);

    final OMException ex = assertThrows(OMException.class, OzoneManager::getS3AuthEffectiveAccessId);
    assertEquals(INVALID_REQUEST, ex.getResult());
    assertEquals("Invalid STS Token format - could not find originalAccessKeyId", ex.getMessage());
  }
}


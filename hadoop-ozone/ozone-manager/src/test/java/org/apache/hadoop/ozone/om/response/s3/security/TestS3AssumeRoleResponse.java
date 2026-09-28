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

package org.apache.hadoop.ozone.om.response.s3.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for S3AssumeRoleResponse.
 */
public class TestS3AssumeRoleResponse {

  @Test
  public void testAddToDBBatchIsNoOpAndResponseIsAccessible() throws Exception {
    final AssumeRoleResponse assumeRoleResponse = AssumeRoleResponse.newBuilder()
        .setAccessKeyId("ASIA123")
        .setSecretAccessKey("secret-xyz")
        .setSessionToken("session-token")
        .setExpirationEpochSeconds(12345L)
        .setAssumedRoleId("AROA123:session")
        .build();

    final OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(Type.AssumeRole)
        .setStatus(Status.OK)
        .setSuccess(true)
        .setAssumeRoleResponse(assumeRoleResponse)
        .build();

    final S3AssumeRoleResponse response = new S3AssumeRoleResponse(omResponse);

    // Should not throw and should not interact with DB tables
    final OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    final BatchOperation batchOperation = mock(BatchOperation.class);
    response.addToDBBatch(omMetadataManager, batchOperation);

    // Ensure the wrapped response is present and unchanged
    assertThat(response.getOMResponse().getStatus()).isEqualTo(Status.OK);
    assertThat(response.getOMResponse().hasAssumeRoleResponse()).isTrue();
    assertThat(response.getOMResponse().getAssumeRoleResponse()).isEqualTo(assumeRoleResponse);

    // Verify that batch operations were never called
    verifyNoInteractions(batchOperation);
    verifyNoInteractions(omMetadataManager);
  }

  @Test
  public void testResponseWithErrorStatus() {
    final OMResponse errorResponse = OMResponse.newBuilder()
        .setCmdType(Type.AssumeRole)
        .setStatus(Status.INVALID_REQUEST)
        .setSuccess(false)
        .build();

    final S3AssumeRoleResponse response = new S3AssumeRoleResponse(errorResponse);

    assertThat(response.getOMResponse().getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(response.getOMResponse().getSuccess()).isFalse();
    assertThat(response.getOMResponse().hasAssumeRoleResponse()).isFalse();
  }

  @Test
  public void testResponsePreservesAllAssumeRoleDetails() {
    final String expectedAccessKeyId = "ASIA123";
    final String expectedSecretAccessKey = "secretAccessKey";
    final String expectedSessionToken = "sessionTokenData";
    final long expectedExpiration = 1234567890L;
    final String expectedAssumedRoleId = "AROA1234567890:mySession";

    final AssumeRoleResponse assumeRoleResponse = AssumeRoleResponse.newBuilder()
        .setAccessKeyId(expectedAccessKeyId)
        .setSecretAccessKey(expectedSecretAccessKey)
        .setSessionToken(expectedSessionToken)
        .setExpirationEpochSeconds(expectedExpiration)
        .setAssumedRoleId(expectedAssumedRoleId)
        .build();

    final OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(Type.AssumeRole)
        .setStatus(Status.OK)
        .setSuccess(true)
        .setAssumeRoleResponse(assumeRoleResponse)
        .build();

    final S3AssumeRoleResponse response = new S3AssumeRoleResponse(omResponse);

    final AssumeRoleResponse retrievedResponse = response.getOMResponse().getAssumeRoleResponse();
    assertThat(retrievedResponse.getAccessKeyId()).isEqualTo(expectedAccessKeyId);
    assertThat(retrievedResponse.getSecretAccessKey()).isEqualTo(expectedSecretAccessKey);
    assertThat(retrievedResponse.getSessionToken()).isEqualTo(expectedSessionToken);
    assertThat(retrievedResponse.getExpirationEpochSeconds()).isEqualTo(expectedExpiration);
    assertThat(retrievedResponse.getAssumedRoleId()).isEqualTo(expectedAssumedRoleId);
  }

  @Test
  public void testResponseWithEmptyAssumeRoleResponse() {
    final OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(Type.AssumeRole)
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();

    final S3AssumeRoleResponse response = new S3AssumeRoleResponse(omResponse);

    assertThat(response.getOMResponse().hasAssumeRoleResponse()).isFalse();
  }
}



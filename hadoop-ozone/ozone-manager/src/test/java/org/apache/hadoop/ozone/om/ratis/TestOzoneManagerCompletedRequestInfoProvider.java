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

package org.apache.hadoop.ozone.om.ratis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.OperationArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Testing OzoneManagerCompletedRequestInfoProvider class.
 */
public class TestOzoneManagerCompletedRequestInfoProvider {

  private static final String TEST_VOLUME_NAME = "testVol";
  private static final String TEST_BUCKET_NAME = "testBucket";
  private static final String TEST_KEY = "/foo/bar/baz/key";
  private static final String TEST_KEY_RENAMED = TEST_KEY + "_RENAMED";

  private static final KeyArgs TEST_KEY_ARGS = KeyArgs.newBuilder()
        .setKeyName(TEST_KEY)
        .setVolumeName(TEST_VOLUME_NAME)
        .setBucketName(TEST_BUCKET_NAME)
        .build();

  protected OMRequest createCreateKeyRequest() {
    CreateKeyRequest createKeyRequest = CreateKeyRequest.newBuilder()
            .setKeyArgs(TEST_KEY_ARGS).build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCreateKeyRequest(createKeyRequest)
        .setCmdType(Type.CreateKey).build();
  }

  protected OMRequest createRenameKeyRequest() {
    RenameKeyRequest renameKeyRequest = RenameKeyRequest.newBuilder()
            .setKeyArgs(TEST_KEY_ARGS).setToKeyName(TEST_KEY_RENAMED).build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setRenameKeyRequest(renameKeyRequest)
        .setCmdType(Type.RenameKey).build();
  }

  @Test
  public void testCreateKeyRequest() throws IOException {

    OzoneManagerCompletedRequestInfoProvider completedRequestInfoProvider
        = new OzoneManagerCompletedRequestInfoProvider();

    OmCompletedRequestInfo requestInfo = completedRequestInfoProvider.get(123L, createCreateKeyRequest());

    assertThat(requestInfo.getTrxLogIndex()).isEqualTo(123L);
    assertThat(requestInfo.getVolumeName()).isEqualTo(TEST_VOLUME_NAME);
    assertThat(requestInfo.getBucketName()).isEqualTo(TEST_BUCKET_NAME);
    assertThat(requestInfo.getKeyName()).isEqualTo(TEST_KEY);
    assertThat(requestInfo.getOpArgs()).isInstanceOf(OperationArgs.CreateKeyArgs.class);
  }

  @Test
  public void testRenameKeyRequest() throws IOException {

    OzoneManagerCompletedRequestInfoProvider completedRequestInfoProvider
        = new OzoneManagerCompletedRequestInfoProvider();

    OmCompletedRequestInfo requestInfo = completedRequestInfoProvider.get(124L, createRenameKeyRequest());

    assertThat(requestInfo.getTrxLogIndex()).isEqualTo(124L);
    assertThat(requestInfo.getVolumeName()).isEqualTo(TEST_VOLUME_NAME);
    assertThat(requestInfo.getBucketName()).isEqualTo(TEST_BUCKET_NAME);
    assertThat(requestInfo.getKeyName()).isEqualTo(TEST_KEY);
    assertThat(requestInfo.getOpArgs()).isInstanceOf(OperationArgs.RenameKeyArgs.class);
    OperationArgs.RenameKeyArgs opArgs = (OperationArgs.RenameKeyArgs) requestInfo.getOpArgs();
    assertThat(opArgs.getToKeyName()).isEqualTo(TEST_KEY_RENAMED);
  }
}

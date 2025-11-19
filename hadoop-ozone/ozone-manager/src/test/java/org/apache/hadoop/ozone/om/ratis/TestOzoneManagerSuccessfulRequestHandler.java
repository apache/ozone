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
package org.apache.hadoop.ozone.om.ratis;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.OperationArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockedConstruction;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

/**
 * Testing OzoneManagerSuccessfulRequestHandler class.
 */
@ExtendWith(MockitoExtension.class)
public class TestOzoneManagerSuccessfulRequestHandler {

  private static final String TEST_VOLUME_NAME = "testVol";
  private static final String TEST_BUCKET_NAME = "testBucket";
  private static final String TEST_KEY = "/foo/bar/baz/key";
  private static final String TEST_KEY_RENAMED = TEST_KEY + "_RENAMED";

  private static final KeyArgs TEST_KEY_ARGS = KeyArgs.newBuilder()
        .setKeyName(TEST_KEY)
        .setVolumeName(TEST_VOLUME_NAME)
        .setBucketName(TEST_BUCKET_NAME)
        .build();

  @Mock
  private OzoneManager ozoneManager;

  @Mock
  private OMMetadataManager omMetadataManager;

  @Mock
  private OzoneConfiguration configuration;

  @Mock
  private DBStore dbStore;

  @Mock
  private BatchOperation batchOperation;

  @Mock
  private Table<String, OmCompletedRequestInfo> completedRequestInfoTable;

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

    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(omMetadataManager.getCompletedRequestInfoTable()).thenReturn(completedRequestInfoTable);
    when(omMetadataManager.getStore()).thenReturn(dbStore);
    when(dbStore.initBatchOperation()).thenReturn(batchOperation);

    OzoneManagerSuccessfulRequestHandler requestHandler
        = new OzoneManagerSuccessfulRequestHandler(ozoneManager);

    requestHandler.handle(123L, createCreateKeyRequest());

    ArgumentCaptor<BatchOperation> arg1 = ArgumentCaptor.forClass(BatchOperation.class);
    ArgumentCaptor<String> arg2 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<OmCompletedRequestInfo> arg3 = ArgumentCaptor.forClass(OmCompletedRequestInfo.class);

    verify(completedRequestInfoTable, times(1)).putWithBatch(arg1.capture(), arg2.capture(), arg3.capture());
    assertThat(arg1.getValue()).isEqualTo(batchOperation);

    String key = arg2.getValue();
    assertThat(key).isEqualTo("00000000000000000123");

    OmCompletedRequestInfo requestInfo = arg3.getValue();
    assertThat(requestInfo.getVolumeName()).isEqualTo(TEST_VOLUME_NAME);
    assertThat(requestInfo.getBucketName()).isEqualTo(TEST_BUCKET_NAME);
    assertThat(requestInfo.getKeyName()).isEqualTo(TEST_KEY);
    assertThat(requestInfo.getOpArgs()).isInstanceOf(OperationArgs.CreateKeyArgs.class);
  }

  @Test
  public void testRenameKeyRequest() throws IOException {

    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(omMetadataManager.getCompletedRequestInfoTable()).thenReturn(completedRequestInfoTable);
    when(omMetadataManager.getStore()).thenReturn(dbStore);
    when(dbStore.initBatchOperation()).thenReturn(batchOperation);

    OzoneManagerSuccessfulRequestHandler requestHandler
        = new OzoneManagerSuccessfulRequestHandler(ozoneManager);

    requestHandler.handle(124L, createRenameKeyRequest());

    ArgumentCaptor<BatchOperation> arg1 = ArgumentCaptor.forClass(BatchOperation.class);
    ArgumentCaptor<String> arg2 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<OmCompletedRequestInfo> arg3 = ArgumentCaptor.forClass(OmCompletedRequestInfo.class);

    verify(completedRequestInfoTable, times(1)).putWithBatch(arg1.capture(), arg2.capture(), arg3.capture());
    assertThat(arg1.getValue()).isEqualTo(batchOperation);

    String key = arg2.getValue();
    assertThat(key).isEqualTo("00000000000000000124");

    OmCompletedRequestInfo requestInfo = arg3.getValue();
    assertThat(requestInfo.getVolumeName()).isEqualTo(TEST_VOLUME_NAME);
    assertThat(requestInfo.getBucketName()).isEqualTo(TEST_BUCKET_NAME);
    assertThat(requestInfo.getKeyName()).isEqualTo(TEST_KEY);
    assertThat(requestInfo.getOpArgs()).isInstanceOf(OperationArgs.RenameKeyArgs.class);
    OperationArgs.RenameKeyArgs opArgs = (OperationArgs.RenameKeyArgs) requestInfo.getOpArgs();
    assertThat(opArgs.getToKeyName()).isEqualTo(TEST_KEY_RENAMED);
  }
}

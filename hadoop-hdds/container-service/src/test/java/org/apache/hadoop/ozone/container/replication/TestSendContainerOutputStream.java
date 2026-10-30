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

package org.apache.hadoop.ozone.container.replication;

import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.verify;

import java.io.OutputStream;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

/**
 * Test for {@link SendContainerOutputStream}.
 */
class TestSendContainerOutputStream
    extends GrpcOutputStreamTest<SendContainerRequest> {

  TestSendContainerOutputStream() {
    super(SendContainerRequest.class);
  }

  @Override
  protected OutputStream createSubject() {
    return new SendContainerOutputStream(getObserver(),
        getContainerId(), getBufferSize(), NO_COMPRESSION, null);
  }

  @ParameterizedTest
  @EnumSource
  void usesCompression(CopyContainerCompression compression) throws Exception {
    OutputStream subject = new SendContainerOutputStream(
        getObserver(), getContainerId(), getBufferSize(), compression, null);

    byte[] bytes = getRandomBytes(16);
    subject.write(bytes, 0, bytes.length);
    subject.close();

    SendContainerRequest req = SendContainerRequest.newBuilder()
        .setContainerID(getContainerId())
        .setOffset(0)
        .setData(ByteString.copyFrom(bytes))
        .setCompression(compression.toProto())
        .build();

    verify(getObserver()).onNext(req);
    verify(getObserver()).onCompleted();
  }

  /**
   * The target storage type travels on the first request, next to size, so the
   * importing datanode can place the container on a matching volume.
   */
  @ParameterizedTest
  @EnumSource(StorageType.class)
  void sendsTargetVolumeStorageType(StorageType storageType) throws Exception {
    OutputStream subject = new SendContainerOutputStream(
        getObserver(), getContainerId(), getBufferSize(), NO_COMPRESSION, null,
        storageType);

    byte[] bytes = getRandomBytes(16);
    subject.write(bytes, 0, bytes.length);
    subject.close();

    SendContainerRequest req = SendContainerRequest.newBuilder()
        .setContainerID(getContainerId())
        .setOffset(0)
        .setData(ByteString.copyFrom(bytes))
        .setCompression(NO_COMPRESSION.toProto())
        .setStorageTypeID(StorageTypeUtils.getID(storageType))
        .build();

    verify(getObserver()).onNext(req);
    verify(getObserver()).onCompleted();
  }

  /**
   * Without a target storage type the request must not claim one, so the target
   * keeps its any-volume behaviour.
   */
  @Test
  void omitsStorageTypeWhenNotRequested() throws Exception {
    OutputStream subject = createSubject();

    byte[] bytes = getRandomBytes(16);
    subject.write(bytes, 0, bytes.length);
    subject.close();

    ArgumentCaptor<SendContainerRequest> captor =
        ArgumentCaptor.forClass(SendContainerRequest.class);
    verify(getObserver()).onNext(captor.capture());
    assertFalse(captor.getValue().hasStorageTypeID());
  }

  @Override
  protected ByteString verifyPart(SendContainerRequest response,
      int expectedOffset, int size) {
    assertEquals(getContainerId(), response.getContainerID());
    assertEquals(expectedOffset, response.getOffset());
    assertEquals(size, response.getData().size());
    return response.getData();
  }
}

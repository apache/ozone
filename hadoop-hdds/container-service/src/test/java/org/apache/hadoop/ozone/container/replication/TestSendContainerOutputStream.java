/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.OutputStream;

import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

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
        getContainerId(), getBufferSize(), NO_COMPRESSION);
  }

  @ParameterizedTest
  @EnumSource
  void usesCompression(CopyContainerCompression compression) throws Exception {
    OutputStream subject = new SendContainerOutputStream(
        getObserver(), getContainerId(), getBufferSize(), compression);

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

  protected ByteString verifyPart(SendContainerRequest response,
      int expectedOffset, int size) {
    assertEquals(getContainerId(), response.getContainerID());
    assertEquals(expectedOffset, response.getOffset());
    assertEquals(size, response.getData().size());
    return response.getData();
  }
}

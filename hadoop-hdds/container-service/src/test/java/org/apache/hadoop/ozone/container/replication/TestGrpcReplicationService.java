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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerResponseProto;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link GrpcReplicationService}.
 */
class TestGrpcReplicationService {

  @Test
  void closesStreamOnError() {
    // GIVEN
    ContainerReplicationSource source = new ContainerReplicationSource() {
      @Override
      public void prepare(long containerId) {
        // no-op
      }

      @Override
      public void copyData(long containerId, OutputStream destination,
          CopyContainerCompression compression) throws IOException {
        throw new IOException("testing");
      }
    };
    ContainerImporter importer = mock(ContainerImporter.class);
    GrpcReplicationService subject =
        new GrpcReplicationService(source, importer);

    CopyContainerRequestProto request = CopyContainerRequestProto.newBuilder()
        .setContainerID(1)
        .setReadOffset(0)
        .setLen(123)
        .build();
    CallStreamObserver<CopyContainerResponseProto> observer =
        mock(CallStreamObserver.class);
    when(observer.isReady()).thenReturn(true);

    // WHEN
    subject.download(request, observer);

    // THEN
    // onCompleted is called by GrpcOutputStream#close
    // so we indirectly verify that the stream is closed
    verify(observer).onCompleted();
  }

}

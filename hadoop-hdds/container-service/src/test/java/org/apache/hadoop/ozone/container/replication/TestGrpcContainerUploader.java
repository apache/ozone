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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.InMemoryConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerResponse;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for {@link GrpcContainerUploader}.
 */
class TestGrpcContainerUploader {

  @Test
  void successfulReplication() throws Exception {
    // GIVEN
    GrpcReplicationClient client = mock(GrpcReplicationClient.class);
    ArgumentCaptor<StreamObserver<SendContainerResponse>> captor =
        ArgumentCaptor.forClass(StreamObserver.class);
    when(client.upload(captor.capture()))
        .thenReturn(new NoopObserver() {
          @Override
          public void onNext(SendContainerRequest value) {
            captor.getValue()
                .onNext(SendContainerResponse.getDefaultInstance());
          }
        });

    CompletableFuture<Void> callback = new CompletableFuture<>();
    GrpcContainerUploader subject = createSubject(client);

    // WHEN
    OutputStream out = startUpload(subject, callback);
    out.close();

    // THEN
    verify(client).close();
  }

  @Test
  void errorInResponse() throws Exception {
    // GIVEN
    GrpcReplicationClient client = mock(GrpcReplicationClient.class);
    ArgumentCaptor<StreamObserver<SendContainerResponse>> captor =
        ArgumentCaptor.forClass(StreamObserver.class);
    when(client.upload(captor.capture()))
        .thenReturn(new NoopObserver() {
          @Override
          public void onNext(SendContainerRequest value) {
            captor.getValue().onError(new RuntimeException("testing"));
          }
        });

    CompletableFuture<Void> callback = new CompletableFuture<>();
    GrpcContainerUploader subject = createSubject(client);

    // WHEN
    OutputStream out = startUpload(subject, callback);
    out.write(RandomUtils.secure().randomBytes(4));
    out.close();

    // THEN
    assertTrue(callback.isCompletedExceptionally());
    verify(client).close();
  }

  @Test
  void immediateError() throws Exception {
    // GIVEN
    GrpcReplicationClient client = mock(GrpcReplicationClient.class);
    when(client.upload(any()))
        .thenThrow(new RuntimeException("testing"));

    CompletableFuture<Void> callback = new CompletableFuture<>();
    GrpcContainerUploader subject = createSubject(client);

    // WHEN
    assertThrows(RuntimeException.class, () -> startUpload(subject, callback));

    // THEN
    verify(client).close();
  }

  private static GrpcContainerUploader createSubject(
      GrpcReplicationClient client) {
    return new GrpcContainerUploader(new InMemoryConfiguration(), null,
        mock(ContainerController.class)) {
      @Override
      protected GrpcReplicationClient createReplicationClient(
          DatanodeDetails target, CopyContainerCompression compression) {
        return client;
      }
    };
  }

  private static OutputStream startUpload(GrpcContainerUploader subject,
      CompletableFuture<Void> callback) throws IOException {
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    return subject.startUpload(1, target, callback, NO_COMPRESSION);
  }

  /**
   * Empty implementation.
   */
  private static class NoopObserver
      extends CallStreamObserver<SendContainerRequest> {

    @Override
    public void onNext(SendContainerRequest value) {
      // override if needed
    }

    @Override
    public void onError(Throwable t) {
      // override if needed
    }

    @Override
    public void onCompleted() {
      // override if needed
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {
    }

    @Override
    public void disableAutoInboundFlowControl() {
    }

    @Override
    public void request(int count) {
    }

    @Override
    public void setMessageCompression(boolean enable) {
    }
  }
}

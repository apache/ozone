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

package org.apache.hadoop.hdds.scm.ha;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos.CopyDBCheckpointRequestProto;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos.CopyDBCheckpointResponseProto;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolServiceGrpc;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.ozone.test.GenericTestUtils.PortAllocator;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link InterSCMGrpcClient}.
 */
class TestInterSCMGrpcClient {

  @TempDir
  private Path temp;

  /**
   * {@link ScmConfigKeys#OZONE_SCM_HA_GRPC_DEADLINE_INTERVAL} is read in
   * milliseconds, so the gRPC deadline has to be declared in milliseconds as
   * well. A seconds declaration inflates the configured deadline 1000 times,
   * and a stuck download is not aborted at the configured interval.
   */
  @Test
  void testDownloadIsAbortedAtConfiguredDeadline() throws Exception {
    int port = PortAllocator.getFreePort();
    Server server = ServerBuilder.forPort(port)
        .addService(new NeverRespondingService())
        .build();
    server.start();

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_HA_GRPC_DEADLINE_INTERVAL, "200ms");

    try (InterSCMGrpcClient client =
        new InterSCMGrpcClient("localhost", port, conf, null)) {
      CompletableFuture<Path> res = client.download(temp.resolve("cpFile"));
      ExecutionException e = assertThrows(ExecutionException.class,
          () -> res.get(10, TimeUnit.SECONDS));
      assertEquals(Status.Code.DEADLINE_EXCEEDED,
          Status.fromThrowable(e.getCause()).getCode());
    } finally {
      server.shutdownNow();
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Keeps the download stream pending, so that the client side deadline is
   * the only way the call can end.
   */
  private static final class NeverRespondingService
      extends InterSCMProtocolServiceGrpc.InterSCMProtocolServiceImplBase {

    @Override
    public void download(CopyDBCheckpointRequestProto request,
        StreamObserver<CopyDBCheckpointResponseProto> responseObserver) {
      // intentionally left pending
    }
  }
}

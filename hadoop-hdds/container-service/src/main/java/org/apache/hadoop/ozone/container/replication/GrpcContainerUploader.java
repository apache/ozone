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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerResponse;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

/**
 * Uploads container to target datanode via gRPC.
 */
public class GrpcContainerUploader implements ContainerUploader {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcContainerUploader.class);

  private final SecurityConfig securityConfig;
  private final CertificateClient certClient;

  public GrpcContainerUploader(
      ConfigurationSource conf, CertificateClient certClient) {
    this.certClient = certClient;
    securityConfig = new SecurityConfig(conf);
  }

  @Override
  public OutputStream startUpload(long containerId, DatanodeDetails target,
      CompletableFuture<Void> callback, CopyContainerCompression compression)
      throws IOException {
    GrpcReplicationClient client =
        new GrpcReplicationClient(target.getIpAddress(),
            target.getPort(Port.Name.REPLICATION).getValue(),
            securityConfig, certClient, compression);
    StreamObserver<SendContainerRequest> requestStream = client.upload(
        new SendContainerResponseStreamObserver(containerId, target, callback));
    return new SendContainerOutputStream(requestStream, containerId,
        GrpcReplicationService.BUFFER_SIZE, compression);
  }

  /**
   *
   */
  private static class SendContainerResponseStreamObserver
      implements StreamObserver<SendContainerResponse> {
    private final long containerId;
    private final DatanodeDetails target;
    private final CompletableFuture<Void> callback;

    SendContainerResponseStreamObserver(long containerId,
        DatanodeDetails target, CompletableFuture<Void> callback) {
      this.containerId = containerId;
      this.target = target;
      this.callback = callback;
    }

    @Override
    public void onNext(SendContainerResponse sendContainerResponse) {
      LOG.info("Response for upload container {} to {}", containerId, target);
    }

    @Override
    public void onError(Throwable t) {
      LOG.warn("Failed to upload container {} to {}", containerId, target, t);
      callback.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      LOG.info("Finished uploading container {} to {}", containerId, target);
      callback.complete(null);
    }
  }
}

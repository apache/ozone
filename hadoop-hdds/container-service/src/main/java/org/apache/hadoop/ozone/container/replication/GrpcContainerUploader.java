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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerResponse;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uploads container to target datanode via gRPC.
 */
public class GrpcContainerUploader implements ContainerUploader {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcContainerUploader.class);

  private final SecurityConfig securityConfig;
  private final CertificateClient certClient;
  private final ContainerController containerController;

  public GrpcContainerUploader(
      ConfigurationSource conf, CertificateClient certClient, 
      ContainerController containerController) {
    this.certClient = certClient;
    this.containerController = containerController;
    securityConfig = new SecurityConfig(conf);
  }

  @Override
  public OutputStream startUpload(long containerId, DatanodeDetails target,
      CompletableFuture<Void> callback, CopyContainerCompression compression) throws IOException {
    
    // Get container size from local datanode instead of using passed replicateSize
    Long containerSize = null;
    Container<?> container = containerController.getContainer(containerId);
    if (container != null) {
      LOG.debug("Starting upload of container {} to {} with size {}",
          containerId, target, container.getContainerData().getBytesUsed());
      containerSize = container.getContainerData().getBytesUsed();
    }
    
    GrpcReplicationClient client = createReplicationClient(target, compression);
    try {
      // gRPC runtime always provides implementation of CallStreamObserver
      // that allows flow control.
      SendContainerResponseStreamObserver responseObserver
          = new SendContainerResponseStreamObserver(
              containerId, target, callback);
      CallStreamObserver<SendContainerRequest> requestStream =
          new WrappedRequestStreamObserver(
              (CallStreamObserver<SendContainerRequest>) client.upload(
              responseObserver), responseObserver);
      return new SendContainerOutputStream(requestStream, containerId,
          GrpcReplicationService.BUFFER_SIZE, compression, containerSize) {
        @Override
        public void close() throws IOException {
          try {
            super.close();
          } finally {
            IOUtils.close(LOG, client);
          }
        }
      };
    } catch (Exception e) {
      IOUtils.close(LOG, client);
      throw e;
    }
  }

  @VisibleForTesting
  protected GrpcReplicationClient createReplicationClient(
      DatanodeDetails target, CopyContainerCompression compression)
      throws IOException {
    return new GrpcReplicationClient(target.getIpAddress(),
        target.getPort(Port.Name.REPLICATION).getValue(),
        securityConfig, certClient, compression);
  }

  /**
   * Observes gRPC response for SendContainer request, notifies callback on
   * completion/error.
   */
  public static class SendContainerResponseStreamObserver
      implements StreamObserver<SendContainerResponse> {
    private final long containerId;
    private final DatanodeDetails target;
    private final CompletableFuture<Void> callback;
    private AtomicBoolean error = new AtomicBoolean(false);
    private volatile Throwable throwable = null;

    SendContainerResponseStreamObserver(long containerId,
        DatanodeDetails target, CompletableFuture<Void> callback) {
      this.containerId = containerId;
      this.target = target;
      this.callback = callback;
    }

    @Override
    public void onNext(SendContainerResponse sendContainerResponse) {
      LOG.debug("Response for upload container {} to {}", containerId, target);
    }

    @Override
    public void onError(Throwable t) {
      LOG.warn("Failed to upload container {} to {}", containerId, target, t);

      throwable = t;
      error.set(true);
      callback.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      LOG.info("Finished uploading container {} to {}", containerId, target);
      callback.complete(null);
    }

    public boolean isError() {
      return error.get();
    }

    public Throwable getError() {
      return throwable;
    }
  }

  /**
   * this class wrap the request stream observer and handle error
   * reported by ratis to response handler.
   */
  public static class WrappedRequestStreamObserver<T>
      extends CallStreamObserver<T> {
    private final CallStreamObserver<T> observer;
    private final SendContainerResponseStreamObserver responseObserver;

    public WrappedRequestStreamObserver(
        CallStreamObserver observer,
        SendContainerResponseStreamObserver responseObserver) {
      this.observer = observer;
      this.responseObserver = responseObserver;
    }

    @Override
    public boolean isReady() {
      if (responseObserver.isError()) {
        throw new RuntimeException(responseObserver.getError());
      }
      return observer.isReady();
    }

    @Override
    public void setOnReadyHandler(Runnable runnable) {
      observer.setOnReadyHandler(runnable);
    }

    @Override
    public void disableAutoInboundFlowControl() {
      observer.disableAutoInboundFlowControl();
    }

    @Override
    public void request(int i) {
      observer.request(i);
    }

    @Override
    public void setMessageCompression(boolean b) {
      observer.setMessageCompression(b);
    }

    @Override
    public void onNext(T sendContainerResponse) {
      observer.onNext(sendContainerResponse);
    }

    @Override
    public void onError(Throwable throwable) {
      if (!responseObserver.isError()) {
        // set up error to response observer, so that
        // callback can be set with error in response observer
        responseObserver.onError(throwable);
      }
      observer.onError(throwable);
    }

    @Override
    public void onCompleted() {
      observer.onCompleted();
    }
  }
}

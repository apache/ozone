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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerResponse;
import org.apache.hadoop.hdds.protocol.datanode.proto.IntraDatanodeProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto.IntraDatanodeProtocolServiceGrpc.IntraDatanodeProtocolServiceStub;
import org.apache.hadoop.hdds.security.ssl.KeyStoresFactory;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.OzoneConsts;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.ClientAuth;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to read container data from gRPC.
 */
public class GrpcReplicationClient implements AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcReplicationClient.class);

  private final ManagedChannel channel;

  private final IntraDatanodeProtocolServiceStub client;

  private final ContainerProtos.CopyContainerCompressProto compression;

  public GrpcReplicationClient(
      String host, int port,
      SecurityConfig secConfig, CertificateClient certClient,
      String compression)
      throws IOException {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);

    if (secConfig.isSecurityEnabled() && secConfig.isGrpcTlsEnabled()) {
      channelBuilder.useTransportSecurity();

      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
      if (certClient != null) {
        KeyStoresFactory factory = certClient.getClientKeyStoresFactory();
        sslContextBuilder
            .trustManager(factory.getTrustManagers()[0])
            .clientAuth(ClientAuth.REQUIRE)
            .keyManager(factory.getKeyManagers()[0]);
      }
      if (secConfig.useTestCert()) {
        channelBuilder.overrideAuthority("localhost");
      }
      channelBuilder.sslContext(sslContextBuilder.build());
    }
    channel = channelBuilder.build();
    client = IntraDatanodeProtocolServiceGrpc.newStub(channel);
    this.compression =
        ContainerProtos.CopyContainerCompressProto.valueOf(compression);
  }

  public CompletableFuture<Path> download(long containerId, Path dir) {
    CopyContainerRequestProto request =
        CopyContainerRequestProto.newBuilder()
            .setContainerID(containerId)
            .setLen(-1)
            .setReadOffset(0)
            .setCompression(compression)
            .build();

    CompletableFuture<Path> response = new CompletableFuture<>();

    Path destinationPath = dir
        .resolve(ContainerUtils.getContainerTarName(containerId));

    client.download(request,
        new StreamDownloader(containerId, response, destinationPath));

    return response;
  }

  public StreamObserver<SendContainerRequest> upload(
      StreamObserver<SendContainerResponse> responseObserver) {
    return client.upload(responseObserver);
  }

  public void shutdown() {
    channel.shutdown();
    try {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("failed to shutdown replication channel", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() throws Exception {
    shutdown();
  }

  /**
   * gRPC stream observer to CompletableFuture adapter.
   */
  public static class StreamDownloader
      implements StreamObserver<CopyContainerResponseProto> {

    private final CompletableFuture<Path> response;
    private final long containerId;
    private final OutputStream stream;
    private final Path outputPath;

    public StreamDownloader(long containerId, CompletableFuture<Path> response,
        Path outputPath) {
      this.response = response;
      this.containerId = containerId;
      this.outputPath = outputPath;
      try {
        Preconditions.checkNotNull(outputPath, "Output path cannot be null");
        Path parentPath = Preconditions.checkNotNull(outputPath.getParent());
        Files.createDirectories(parentPath);
        stream = new FileOutputStream(outputPath.toFile());
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Output path can't be used: " + outputPath, e);
      }
    }

    @Override
    public void onNext(CopyContainerResponseProto chunk) {
      try {
        chunk.getData().writeTo(stream);
      } catch (IOException e) {
        LOG.error("Failed to write the stream buffer to {} for container {}",
            outputPath, containerId, e);
        try {
          stream.close();
        } catch (IOException ex) {
          LOG.error("Failed to close OutputStream {}", outputPath, e);
        } finally {
          deleteOutputOnFailure();
          response.completeExceptionally(e);
        }
      }
    }

    @Override
    public void onError(Throwable throwable) {
      try {
        LOG.error("Download of container {} was unsuccessful",
            containerId, throwable);
        stream.close();
        deleteOutputOnFailure();
        response.completeExceptionally(throwable);
      } catch (IOException e) {
        LOG.error("Failed to close {} for container {}",
            outputPath, containerId, e);
        deleteOutputOnFailure();
        response.completeExceptionally(e);
      }
    }

    @Override
    public void onCompleted() {
      try {
        stream.close();
        LOG.info("Container {} is downloaded to {}", containerId, outputPath);
        response.complete(outputPath);
      } catch (IOException e) {
        LOG.error("Downloaded container {} OK, but failed to close {}",
            containerId, outputPath, e);
        deleteOutputOnFailure();
        response.completeExceptionally(e);
      }
    }

    private void deleteOutputOnFailure() {
      try {
        Files.delete(outputPath);
      } catch (IOException ex) {
        LOG.error("Failed to delete temporary destination {} for " +
                "unsuccessful download of container {}",
            outputPath, containerId, ex);
      }
    }
  }
}

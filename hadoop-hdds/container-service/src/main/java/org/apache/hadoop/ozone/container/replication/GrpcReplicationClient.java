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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CopyContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ListBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkVersion;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.IntraDatanodeProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto.IntraDatanodeProtocolServiceGrpc.IntraDatanodeProtocolServiceStub;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ozone.OzoneConsts;

import com.google.common.base.Preconditions;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
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

  private final Path workingDirectory;

  public GrpcReplicationClient(
      String host, int port, Path workingDir,
      SecurityConfig secConfig, CertificateClient certClient
  ) throws IOException {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);

    if (secConfig.isSecurityEnabled()) {
      channelBuilder.useTransportSecurity();

      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
      if (certClient != null) {
        sslContextBuilder
            .trustManager(HAUtils.buildCAX509List(certClient,
                secConfig.getConfiguration()))
            .clientAuth(ClientAuth.REQUIRE)
            .keyManager(certClient.getPrivateKey(),
                certClient.getCertificate());
      }
      if (secConfig.useTestCert()) {
        channelBuilder.overrideAuthority("localhost");
      }
      channelBuilder.sslContext(sslContextBuilder.build());
    }
    channel = channelBuilder.build();
    client = IntraDatanodeProtocolServiceGrpc.newStub(channel);
    workingDirectory = workingDir;
  }

  public CompletableFuture<Path> download(long containerId) {
    CopyContainerRequestProto request =
        CopyContainerRequestProto.newBuilder()
            .setContainerID(containerId)
            .setLen(-1)
            .setReadOffset(0)
            .build();

    CompletableFuture<Path> response = new CompletableFuture<>();

    Path destinationPath =
        getWorkingDirectory().resolve("container-" + containerId + ".tar.gz");

    client.download(request,
        new StreamDownloader(containerId, response, destinationPath));

    return response;
  }

  public CompletableFuture<List<BlockData>> listBlock(
      long containerId, String datanodeUUID, int count) {
    return listBlock(containerId, datanodeUUID, -1, count);
  }

  public CompletableFuture<List<BlockData>> listBlock(
      long containerId, String datanodeUUID, long start, int count) {

    ListBlockRequestProto.Builder requestBuilder =
        ListBlockRequestProto.newBuilder()
            .setCount(count);
    if (start >= 0) {
      requestBuilder.setStartLocalID(start);
    }
    ListBlockRequestProto request = requestBuilder.build();

    ContainerCommandRequestProto command =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.ListBlock)
            .setContainerID(containerId)
            .setDatanodeUuid(datanodeUUID)
            .setListBlock(request)
            .build();

    CompletableFuture<List<ContainerCommandResponseProto>> future =
        new CompletableFuture<>();

    client.send(command, new ContainerCommandObserver(containerId, future));

    return future.thenApply(responses -> responses.stream()
        .filter(r -> r.getCmdType() == Type.ListBlock)
        .flatMap(r -> r.getListBlock().getBlockDataList().stream())
        .collect(Collectors.toList()));
  }

  public CompletableFuture<Optional<ContainerDataProto>> readContainer(
      long containerId, String datanodeUUID) {

    ContainerCommandRequestProto command =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.ReadContainer)
            .setContainerID(containerId)
            .setDatanodeUuid(datanodeUUID)
            .setReadContainer(ReadContainerRequestProto.newBuilder().build())
            .build();

    CompletableFuture<List<ContainerCommandResponseProto>> future =
        new CompletableFuture<>();

    client.send(command, new ContainerCommandObserver(containerId, future));

    return future.thenApply(responses -> responses.stream()
        .filter(r -> r.getCmdType() == Type.ReadContainer)
        .findFirst()
        .map(r -> r.getReadContainer().getContainerData()));
  }

  public CompletableFuture<Optional<ByteBuffer>> readChunk(long containerId,
      String datanodeUUID, DatanodeBlockID blockID, ChunkInfo chunkInfo) {

    ReadChunkRequestProto request =
        ReadChunkRequestProto.newBuilder()
            .setBlockID(blockID)
            .setChunkData(chunkInfo)
            .setReadChunkVersion(ReadChunkVersion.V0)
            .build();

    ContainerCommandRequestProto command =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.ReadChunk)
            .setContainerID(containerId)
            .setDatanodeUuid(datanodeUUID)
            .setReadChunk(request)
            .build();

    CompletableFuture<List<ContainerCommandResponseProto>> future =
        new CompletableFuture<>();

    client.send(command, new ContainerCommandObserver(containerId, future));

    return future.thenApply(responses -> responses.stream()
        .filter(r -> r.getCmdType() == Type.ReadChunk)
        .findFirst()
        .map(r -> r.getReadChunk().getData().asReadOnlyByteBuffer()));
  }

  public CompletableFuture<Long> writeChunk(DatanodeBlockID blockID,
      ChunkInfo chunkInfo, ByteBuffer chunkData) {

    WriteChunkRequestProto request = WriteChunkRequestProto.newBuilder()
        .setBlockID(blockID)
        .setChunkData(chunkInfo)
        .setData(ByteString.copyFrom(chunkData))
        .build();

    CompletableFuture<Long> future = new CompletableFuture<>();

    client.push(request, new PushChunkObserver(future));

    return future;
  }

  private Path getWorkingDirectory() {
    return workingDirectory;
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

  /**
   * gRPC stream observer to CompletableFuture adapter.
   */
  public static class ContainerCommandObserver
      implements StreamObserver<ContainerCommandResponseProto> {

    private final List<ContainerCommandResponseProto> result;
    private final CompletableFuture<List<ContainerCommandResponseProto>> future;
    private final long containerId;

    public ContainerCommandObserver(long containerId,
        CompletableFuture<List<ContainerCommandResponseProto>> future) {
      this.future = future;
      this.containerId = containerId;
      this.result = new ArrayList<>();
    }

    @Override
    public void onNext(ContainerCommandResponseProto response) {
      result.add(response);
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error("Command to container {} failed", containerId, throwable);
      future.completeExceptionally(throwable);
    }

    @Override
    public void onCompleted() {
      future.complete(result);
    }

  }

  /**
   * gRPC stream observer to CompletableFuture adapter.
   */
  public static class PushChunkObserver
      implements StreamObserver<WriteChunkResponseProto> {

    private final CompletableFuture<Long> future;
    private long count;

    PushChunkObserver(CompletableFuture<Long> future) {
      this.future = future;
      this.count = 0;
    }

    @Override
    public void onNext(WriteChunkResponseProto response) {
      count++;
    }

    @Override
    public void onError(Throwable throwable) {
      future.completeExceptionally(throwable);
    }

    @Override
    public void onCompleted() {
      future.complete(count);
    }
  }

}

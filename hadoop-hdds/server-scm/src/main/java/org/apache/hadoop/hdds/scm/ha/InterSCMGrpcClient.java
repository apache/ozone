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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos.CopyDBCheckpointResponseProto;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolServiceGrpc;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Grpc client to download a Rocks db checkpoint from leader node
 * in SCM HA ring.
 */
public class InterSCMGrpcClient implements SCMSnapshotDownloader {
  private static final Logger LOG =
      LoggerFactory.getLogger(InterSCMGrpcClient.class);

  private final ManagedChannel channel;

  private final InterSCMProtocolServiceGrpc.InterSCMProtocolServiceStub
      client;
  private final long timeout;

  public InterSCMGrpcClient(final String host,
      int port, final ConfigurationSource conf,
      CertificateClient scmCertificateClient) throws IOException {
    Objects.requireNonNull(conf, "conf == null");
    timeout = conf.getTimeDuration(
            ScmConfigKeys.OZONE_SCM_HA_GRPC_DEADLINE_INTERVAL,
            ScmConfigKeys.OZONE_SCM_HA_GRPC_DEADLINE_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(host, port).usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
            .proxyDetector(uri -> null);
    SecurityConfig securityConfig = new SecurityConfig(conf);
    if (securityConfig.isSecurityEnabled()
        && securityConfig.isGrpcTlsEnabled()) {
      SslContextBuilder sslClientContextBuilder = SslContextBuilder.forClient();
      sslClientContextBuilder.keyManager(scmCertificateClient.getKeyManager());
      sslClientContextBuilder.trustManager(scmCertificateClient.getTrustManager());
      SslContextBuilder sslContextBuilder = GrpcSslContexts.configure(
          sslClientContextBuilder, securityConfig.getGrpcSslProvider());
      channelBuilder.sslContext(sslContextBuilder.build())
          .useTransportSecurity();
    }

    channel = channelBuilder.build();
    client = InterSCMProtocolServiceGrpc.newStub(channel).
        withDeadlineAfter(timeout, TimeUnit.SECONDS);
  }

  @Override
  public CompletableFuture<Path> download(final Path outputPath) {
    // By default, on every checkpoint, the rocks db will be flushed
    InterSCMProtocolProtos.CopyDBCheckpointRequestProto request =
        InterSCMProtocolProtos.CopyDBCheckpointRequestProto.newBuilder()
            .setFlush(true)
            .build();
    CompletableFuture<Path> response = new CompletableFuture<>();


    client.download(request,
        new StreamDownloader(response, outputPath));

    return response;
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
  public void close() {
    shutdown();
  }

  /**
   * gRPC stream observer to CompletableFuture adapter.
   */
  public static class StreamDownloader
      implements StreamObserver<CopyDBCheckpointResponseProto> {

    private final CompletableFuture<Path> response;
    private final OutputStream stream;
    private final Path outputPath;

    public StreamDownloader(CompletableFuture<Path> response,
        Path outputPath) {
      this.response = response;
      this.outputPath = outputPath;
      try {
        Objects.requireNonNull(outputPath, "Output path cannot be null");
        stream = Files.newOutputStream(outputPath);
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Output path can't be used: " + outputPath, e);
      }
    }

    @Override
    public void onNext(CopyDBCheckpointResponseProto checkPoint) {
      try {
        checkPoint.getData().writeTo(stream);
      } catch (IOException e) {
        onError(e);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      try {
        LOG.error("Download of checkpoint {} was unsuccessful",
            outputPath, throwable);
        stream.close();
        deleteOutputOnFailure();
        response.completeExceptionally(throwable);
      } catch (IOException e) {
        LOG.error("Failed to close {}}",
            outputPath, e);
        response.completeExceptionally(e);
      }
    }

    @Override
    public void onCompleted() {
      try {
        stream.close();
        LOG.info("Checkpoint is downloaded to {}", outputPath);
        response.complete(outputPath);
      } catch (IOException e) {
        LOG.error("Downloaded checkpoint OK, but failed to close {}",
            outputPath, e);
        response.completeExceptionally(e);
      }

    }

    private void deleteOutputOnFailure() {
      try {
        Files.delete(outputPath);
      } catch (IOException ex) {
        LOG.error("Failed to delete destination {} for " +
                "unsuccessful download",
            outputPath, ex);
      }
    }
  }

}

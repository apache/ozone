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

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import static org.apache.hadoop.hdds.conf.ConfigTag.MANAGEMENT;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.tracing.GrpcServerInterceptor;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptors;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.ClientAuth;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SupportedCipherSuiteFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Separated network server for server2server container replication.
 */
public class ReplicationServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationServer.class);

  private Server server;

  private SecurityConfig secConf;

  private CertificateClient caClient;

  private int port;
  private final ContainerImporter importer;

  private ThreadPoolExecutor executor;

  public ReplicationServer(ReplicationConfig replicationConfig,
      SecurityConfig secConf, CertificateClient caClient,
      ContainerImporter importer, String threadNamePrefix) {
    this.secConf = secConf;
    this.caClient = caClient;
    this.importer = importer;
    this.port = replicationConfig.getPort();

    int replicationServerWorkers =
        replicationConfig.getReplicationMaxStreams();
    int replicationQueueLimit =
        replicationConfig.getReplicationQueueLimit();
    LOG.info("Initializing replication server with thread count = {}"
            + " queue length = {}",
        replicationConfig.getReplicationMaxStreams(),
        replicationConfig.getReplicationQueueLimit());
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(threadNamePrefix + "ReplicationContainerReader-%d")
        .build();
    this.executor = new ThreadPoolExecutor(
        replicationServerWorkers,
        replicationServerWorkers,
        60,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(replicationQueueLimit),
        threadFactory);

    init();
  }

  public void init() {
    GrpcReplicationService grpcReplicationService = new GrpcReplicationService(importer);
    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
        .addService(ServerInterceptors.intercept(
            grpcReplicationService.bindServiceWithZeroCopy(),
            new GrpcServerInterceptor()))
        .executor(executor);

    if (secConf.isSecurityEnabled() && secConf.isGrpcTlsEnabled()) {
      try {
        SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(caClient.getKeyManager());

        sslContextBuilder = GrpcSslContexts.configure(
            sslContextBuilder, secConf.getGrpcSslProvider());

        sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
        sslContextBuilder.trustManager(caClient.getTrustManager());
        sslContextBuilder.protocols(secConf.getGrpcTlsProtocols());
        sslContextBuilder.ciphers(
            secConf.getGrpcTlsCiphers(),
            SupportedCipherSuiteFilter.INSTANCE);

        nettyServerBuilder.sslContext(sslContextBuilder.build());
      } catch (IOException ex) {
        throw new IllegalArgumentException(
            "Unable to setup TLS for secure datanode replication GRPC "
                + "endpoint.", ex);
      }
    }

    server = nettyServerBuilder.build();
  }

  public void start() throws IOException {
    server.start();
    port = server.getPort();
    LOG.info("{} is started using port {}", getClass().getSimpleName(), port);
  }

  public void stop() {
    try {
      executor.shutdown();
      executor.awaitTermination(5L, TimeUnit.SECONDS);
      server.shutdown().awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      LOG.warn("{} couldn't be stopped gracefully", getClass().getSimpleName());
      Thread.currentThread().interrupt();
    }
  }

  public int getPort() {
    return port;
  }

  public void setPoolSize(int size) {
    HddsServerUtil.setPoolSize(executor, size, LOG);
  }

  @VisibleForTesting
  public ThreadPoolExecutor getExecutor() {
    return executor;
  }

  /**
   * Replication-related configuration.
   */
  @ConfigGroup(prefix = ReplicationConfig.PREFIX)
  public static final class ReplicationConfig {

    public static final String PREFIX = "hdds.datanode.replication";
    public static final String STREAMS_LIMIT_KEY = "streams.limit";

    public static final String REPLICATION_STREAMS_LIMIT_KEY =
        PREFIX + "." + STREAMS_LIMIT_KEY;

    public static final int REPLICATION_MAX_STREAMS_DEFAULT = 10;
    private static final String OUTOFSERVICE_FACTOR_KEY =
        "outofservice.limit.factor";
    static final double OUTOFSERVICE_FACTOR_MIN = 1;
    static final double OUTOFSERVICE_FACTOR_DEFAULT = 2;
    private static final String OUTOFSERVICE_FACTOR_DEFAULT_VALUE = "2.0";
    static final double OUTOFSERVICE_FACTOR_MAX = 10;
    static final String REPLICATION_OUTOFSERVICE_FACTOR_KEY =
        PREFIX + "." + OUTOFSERVICE_FACTOR_KEY;

    public static final String PER_VOLUME_ENABLED_KEY =
        PREFIX + ".per.volume.enabled";
    public static final String PER_VOLUME_STREAMS_LIMIT_KEY =
        PREFIX + ".per.volume.streams.limit";
    public static final int PER_VOLUME_STREAMS_LIMIT_DEFAULT = 2;

    /**
     * Base size of the global replication handler executor and inbound
     * replication server executor.
     */
    @Config(key = "hdds.datanode.replication.streams.limit",
        type = ConfigType.INT,
        defaultValue = "10",
        tags = {DATANODE},
        description = "Sets both the base size of the global replication "
            + "handler executor and the inbound replication server executor. "
            + "The global executor is subject to outofservice.limit.factor "
            + "scaling. When "
            + "hdds.datanode.replication.per.volume.enabled is false (default), "
            + "all source-side replication tasks use the global executor. "
            + "When per.volume.enabled is true, per-volume executors handle "
            + "normal source-side push tasks, while this limit still applies "
            + "to non-push and fallback source tasks and target-side inbound "
            + "push requests."
    )
    private int replicationMaxStreams = REPLICATION_MAX_STREAMS_DEFAULT;

    /**
     * The maximum of replication request queue length.
     */
    @Config(key = "hdds.datanode.replication.queue.limit",
        type = ConfigType.INT,
        defaultValue = "4096",
        tags = {DATANODE},
        description = "The maximum number of queued requests for container " +
            "replication"
    )
    private int replicationQueueLimit = 4096;

    @Config(key = "hdds.datanode.replication.port", defaultValue = "9886",
        description = "Port used for the server2server replication server",
        tags = {DATANODE, MANAGEMENT})
    private int port;

    @Config(key = "hdds.datanode.replication.outofservice.limit.factor",
        type = ConfigType.DOUBLE,
        defaultValue = OUTOFSERVICE_FACTOR_DEFAULT_VALUE,
        tags = {DATANODE, SCM},
        description = "Decommissioning and maintenance nodes can handle more" +
            "replication commands than in-service nodes due to reduced load. " +
            "This multiplier determines the increased queue capacity and " +
            "executor pool size."
    )
    private double outOfServiceFactor = OUTOFSERVICE_FACTOR_DEFAULT;

    @Config(key = PER_VOLUME_ENABLED_KEY,
        type = ConfigType.BOOLEAN,
        defaultValue = "false",
        tags = {DATANODE},
        description = "When true, push-based container replication uses a " +
            "separate replication handler thread pool per data volume so " +
            "that slow replication on one disk does not block replication " +
            "on other disks. Pull replication and other replication tasks " +
            "continue to use the global replication handler thread pool."
    )
    private boolean perVolumeEnabled = false;

    @Config(key = PER_VOLUME_STREAMS_LIMIT_KEY,
        type = ConfigType.INT,
        defaultValue = "2",
        reconfigurable = true,
        tags = {DATANODE},
        description = "When hdds.datanode.replication.per.volume.enabled is "
            + "true, maximum concurrent push replication commands per data "
            + "volume (each volume has its own handler thread pool; effective "
            + "push parallelism on the datanode is roughly the number of "
            + "volumes times this limit, with outofservice.limit.factor "
            + "applied per pool on decommissioning or maintenance nodes). "
            + "Push replication is usually disk-bound, so one or two "
            + "concurrent transfers per volume often saturates the disk."
    )
    private int perVolumeStreamsLimit = PER_VOLUME_STREAMS_LIMIT_DEFAULT;

    public double getOutOfServiceFactor() {
      return outOfServiceFactor;
    }

    public int scaleOutOfServiceLimit(int original) {
      return (int) Math.ceil(original * outOfServiceFactor);
    }

    public int getPort() {
      return port;
    }

    public ReplicationConfig setPort(int portParam) {
      this.port = portParam;
      return this;
    }

    public int getReplicationMaxStreams() {
      return replicationMaxStreams;
    }

    public void setReplicationMaxStreams(int replicationMaxStreams) {
      this.replicationMaxStreams = replicationMaxStreams;
    }

    public int getReplicationQueueLimit() {
      return replicationQueueLimit;
    }

    public void setReplicationQueueLimit(int limit) {
      this.replicationQueueLimit = limit;
    }

    public boolean isPerVolumeEnabled() {
      return perVolumeEnabled;
    }

    public void setPerVolumeEnabled(boolean enabled) {
      this.perVolumeEnabled = enabled;
    }

    public int getPerVolumeStreamsLimit() {
      return perVolumeStreamsLimit;
    }

    public void setPerVolumeStreamsLimit(int limit) {
      this.perVolumeStreamsLimit = limit;
    }

    @PostConstruct
    public void validate() {
      if (replicationMaxStreams < 1) {
        LOG.warn(REPLICATION_STREAMS_LIMIT_KEY + " must be greater than zero " +
                "and was set to {}. Defaulting to {}",
            replicationMaxStreams, REPLICATION_MAX_STREAMS_DEFAULT);
        replicationMaxStreams = REPLICATION_MAX_STREAMS_DEFAULT;
      }

      if (outOfServiceFactor < OUTOFSERVICE_FACTOR_MIN ||
          outOfServiceFactor > OUTOFSERVICE_FACTOR_MAX) {
        double clamped = Math.min(OUTOFSERVICE_FACTOR_MAX,
            Math.max(OUTOFSERVICE_FACTOR_MIN, outOfServiceFactor));
        LOG.warn(
            "{} must be between {} and {} but was set to {}. Clamping to {}",
            REPLICATION_OUTOFSERVICE_FACTOR_KEY,
            OUTOFSERVICE_FACTOR_MIN,
            OUTOFSERVICE_FACTOR_MAX,
            outOfServiceFactor,
            clamped);
        outOfServiceFactor = clamped;
      }

      if (perVolumeStreamsLimit < 1) {
        LOG.warn(PER_VOLUME_STREAMS_LIMIT_KEY + " must be greater than zero " +
                "and was set to {}. Defaulting to {}",
            perVolumeStreamsLimit, PER_VOLUME_STREAMS_LIMIT_DEFAULT);
        perVolumeStreamsLimit = PER_VOLUME_STREAMS_LIMIT_DEFAULT;
      }
    }

  }

}

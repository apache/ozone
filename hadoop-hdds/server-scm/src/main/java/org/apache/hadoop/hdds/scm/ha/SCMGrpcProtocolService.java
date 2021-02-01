/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to serve SCM DB checkpoints available for SCM HA.
 * Ideally should only be run on a ratis leader.
 */
public class SCMGrpcProtocolService  {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMGrpcService.class);

  private final int port;
  private Server server;
  private boolean isStarted = false;

  public SCMGrpcProtocolService(final ConfigurationSource conf,
      final StorageContainerManager scm) {
    Preconditions.checkNotNull(conf);
    this.port = conf.getObject(SCMHAConfiguration.class).getGrpcBindPort();

    NettyServerBuilder nettyServerBuilder =
        ((NettyServerBuilder) ServerBuilder.forPort(port))
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);

    SCMGrpcService service = new SCMGrpcService(scm);
    nettyServerBuilder.addService(service);
    server = nettyServerBuilder.build();
  }

  public int getPort() {
    return this.port;
  }

  public void start() throws IOException {
    if (!isStarted) {
      server.start();
      LOG.info("Starting SCM Grpc Service for port {}", server.getPort());
      isStarted = true;
    }
  }

  public void stop() {
    if (isStarted) {
      server.shutdown();
      try {
        server.awaitTermination(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.error("failed to shutdown XceiverServerGrpc", e);
      }
      isStarted = false;
    }
  }
}
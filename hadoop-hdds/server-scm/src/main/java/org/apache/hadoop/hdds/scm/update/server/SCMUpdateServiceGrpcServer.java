/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.update.server;

import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos;
import org.apache.hadoop.hdds.scm.update.client.CRLStore;
import org.apache.hadoop.hdds.scm.update.client.UpdateServiceConfig;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * gRPC server for SCM update services.
 */
public class SCMUpdateServiceGrpcServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMUpdateServiceGrpcServer.class);

  private static final String SERVICE_NAME = "SCMUpdateService";
  private CRLStore crlStore;
  private int port;
  private Server server;
  private SCMUpdateServiceImpl scmUpdateService;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  public SCMUpdateServiceGrpcServer(final UpdateServiceConfig updateConf,
      final CRLStore crlStore) {
    this.crlStore = crlStore;
    this.port = updateConf.getPort();
  }

  public int getPort() {
    return this.port;
  }

  public void start() throws IOException {
    LOG.info("{} starting", SERVICE_NAME);
    scmUpdateService = new SCMUpdateServiceImpl(crlStore);
    server = ServerBuilder.forPort(port).
        addService(scmUpdateService)
        .build();

    if (!isStarted.compareAndSet(false, true)) {
      LOG.info("Ignoring start() since {} has already started.", SERVICE_NAME);
      return;
    } else {
      server.start();
    }
  }

  public void stop() {
    LOG.info("{} stopping", SERVICE_NAME);
    if (isStarted.get()) {
      scmUpdateService = null;
      server.shutdown();
      try {
        server.awaitTermination(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.error("failed to shutdown SCMClientGrpcServer", e);
      } finally {
        server.shutdownNow();
      }
      LOG.info("{} stopped!", SERVICE_NAME);
      isStarted.set(false);
    }
  }

  public void join() throws InterruptedException {
    while (isStarted.get()) {
      wait();
    }
  }

  public void notifyCrlUpdate() {
    scmUpdateService.notifyUpdate(SCMUpdateServiceProtos.Type.CRLUpdate);
  }

}

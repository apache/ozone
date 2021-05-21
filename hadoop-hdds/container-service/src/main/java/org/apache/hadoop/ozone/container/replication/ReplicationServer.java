/**
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

import java.io.IOException;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.stream.StreamingServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Separated network server for server2server container replication.
 */
public class ReplicationServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationServer.class);

  private SecurityConfig secConf;

  private CertificateClient caClient;

  private ContainerSet containerSet;

  private int port;

  private StreamingServer server;

  public ReplicationServer(
      ContainerSet containerSet,
      ReplicationConfig replicationConfig,
      SecurityConfig secConf,
      CertificateClient caClient
  ) {
    this.secConf = secConf;
    this.caClient = caClient;
    this.containerSet = containerSet;
    this.port = replicationConfig.getPort();
    init();
  }

  public void init() {
    server = new StreamingServer(new ContainerStreamingSource(containerSet),
        this.port);
  }

  public void start() throws IOException {
    try {
      server.start();
    } catch (InterruptedException e) {
      throw new RuntimeException("Couldn't start replication server", e);
    }
  }

  public void stop() {
    server.stop();
  }

  public int getPort() {
    return server.getPort();
  }

  @ConfigGroup(prefix = "hdds.datanode.replication")
  public static final class ReplicationConfig {

    @Config(key = "port", defaultValue = "9886", description = "Port used for"
        + " the server2server replication server", tags = {
        ConfigTag.MANAGEMENT})
    private int port;

    public int getPort() {
      return port;
    }

    public ReplicationConfig setPort(int portParam) {
      this.port = portParam;
      return this;
    }
  }
}

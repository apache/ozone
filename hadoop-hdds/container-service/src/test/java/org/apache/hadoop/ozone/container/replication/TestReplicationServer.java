/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.replication;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.junit.Test;

import java.io.IOException;

/**
 * Test the replication server.
 */
public class TestReplicationServer {

  @Test
  public void testEpollEnabled() {
    OzoneConfiguration config = new OzoneConfiguration();
    DatanodeConfiguration dnConf =
        config.getObject(DatanodeConfiguration.class);
    ReplicationServer.ReplicationConfig replicationConfig =
        config.getObject(ReplicationServer.ReplicationConfig.class);
    SecurityConfig secConf = new SecurityConfig(config);
    ReplicationServer server = new ReplicationServer(null, replicationConfig,
        secConf, dnConf, null);
    try {
      server.start();
      server.stop();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testEpollDisabled() {
    OzoneConfiguration config = new OzoneConfiguration();
    DatanodeConfiguration dnConf =
        config.getObject(DatanodeConfiguration.class);
    ReplicationServer.ReplicationConfig replicationConfig =
        config.getObject(ReplicationServer.ReplicationConfig.class);
    SecurityConfig secConf = new SecurityConfig(config);
    System.setProperty(
        "org.apache.ratis.thirdparty.io.netty.transport.noNative", "true");
    ReplicationServer server = new ReplicationServer(null, replicationConfig,
        secConf, dnConf, null);
    try {
      server.start();
      server.stop();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.ratis;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ratis.conf.RaftProperties;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test RatisHelper class.
 */
public class TestRatisHelper {

  @Test
  public void testCreateRaftClientProperties() {

    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set("hdds.ratis.raft.client.rpc.watch" +
        ".request.timeout", "30s");
    ozoneConfiguration.set("hdds.ratis.raft.client.rpc.request" +
        ".timeout", "30s");
    ozoneConfiguration.set(
        "hdds.ratis.raft.server.rpc.watch.request.timeout", "30s");

    RaftProperties raftProperties = new RaftProperties();
    RatisHelper.createRaftClientProperties(ozoneConfiguration, raftProperties);

    Assert.assertEquals("30s",
        raftProperties.get("raft.client.rpc.watch.request.timeout"));
    Assert.assertEquals("30s",
        raftProperties.get("raft.client.rpc.request.timeout"));
    Assert.assertNull(
        raftProperties.get("raft.server.rpc.watch.request.timeout"));

  }

  @Test
  public void testCreateRaftGrpcPropertiesForClient() {

    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set("hdds.ratis.raft.grpc.message.size.max",
        "30MB");
    ozoneConfiguration.set("hdds.ratis.raft.grpc.flow.control" +
        ".window", "1MB");
    ozoneConfiguration.set("hdds.ratis.raft.grpc.tls.enabled", "true");
    ozoneConfiguration.set("hdds.ratis.raft.grpc.tls.mutual_authn" +
        ".enabled", "true");
    ozoneConfiguration.set("hdds.ratis.raft.grpc.server.port", "100");

    RaftProperties raftProperties = new RaftProperties();
    RatisHelper.createRaftClientProperties(ozoneConfiguration, raftProperties);

    Assert.assertEquals("30MB",
        raftProperties.get("raft.grpc.message.size.max"));
    Assert.assertEquals("1MB",
        raftProperties.get("raft.grpc.flow.control.window"));

    // As we dont match tls and server raft.grpc properties. So they should
    // be null.
    Assert.assertNull(raftProperties.get("raft.grpc.tls.set"));
    Assert.assertNull(raftProperties.get("raft.grpc.tls.mutual_authn.enabled"));
    Assert.assertNull(raftProperties.get("raft.grpc.server.port"));

  }


  @Test
  public void testCreateRaftGrpcPropertiesForServer() {

    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set("hdds.ratis.raft.grpc.message.size.max",
        "30MB");
    ozoneConfiguration.set("hdds.ratis.raft.grpc.flow.control" +
        ".window", "1MB");
    ozoneConfiguration.set("hdds.ratis.raft.grpc.tls.enabled",
        "true");
    ozoneConfiguration.set("hdds.ratis.raft.grpc.tls.mutual_authn" +
        ".enabled", "true");
    ozoneConfiguration.set("hdds.ratis.raft.grpc.server.port",
        "100");

    RaftProperties raftProperties = new RaftProperties();
    RatisHelper.createRaftServerProperties(ozoneConfiguration,
        raftProperties);

    Assert.assertEquals("30MB",
        raftProperties.get("raft.grpc.message.size.max"));
    Assert.assertEquals("1MB",
        raftProperties.get("raft.grpc.flow.control.window"));
    Assert.assertEquals("true",
        raftProperties.get("raft.grpc.tls.enabled"));
    Assert.assertEquals("true",
        raftProperties.get("raft.grpc.tls.mutual_authn.enabled"));
    Assert.assertEquals("100",
        raftProperties.get("raft.grpc.server.port"));

  }

  @Test
  public void testCreateRaftServerProperties() {

    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(
        "hdds.ratis.raft.server.rpc.watch.request.timeout", "30s");
    ozoneConfiguration.set(
        "hdds.ratis.raft.server.rpc.request.timeout", "30s");
    ozoneConfiguration.set(
        "hdds.ratis.raft.client.rpc.request.timeout", "30s");

    RaftProperties raftProperties = new RaftProperties();
    RatisHelper.createRaftServerProperties(ozoneConfiguration, raftProperties);

    Assert.assertEquals("30s",
        raftProperties.get("raft.server.rpc.watch.request.timeout"));
    Assert.assertEquals("30s",
        raftProperties.get("raft.server.rpc.request.timeout"));
    Assert.assertNull(raftProperties.get("raft.client.rpc.request.timeout"));

  }
}

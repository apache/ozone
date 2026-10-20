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

package org.apache.hadoop.ozone.om.jobworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedJobWorkDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerDetailsProto;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.jobworker.client.JobworkerClient;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for Job Worker RPC Protocol.
 */
public class TestJobworkerRPCProtocol {

  private static JobworkerGrpcServer server;
  private static JobworkerClient client;

  @TempDir
  private Path folder;
  private OzoneManager ozoneManager;
  private HddsProtos.UUID jwUuid;
  private JobworkerDetailsProto jobworkerDetailsProto;
  private String omServiceId;

  @BeforeEach
  public void setUp()
      throws IOException, AuthenticationException, InterruptedException, TimeoutException {

    OzoneConfiguration conf = createNewTestPath();
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();
    omServiceId = ozoneManager.getOMServiceId();

    conf.setInt("ozone.om.jobworker.grpc.port", ozoneManager.getJobworkerGrpcPort());
    client = new JobworkerClient("localhost", conf);

    jwUuid = HddsProtos.UUID.newBuilder()
        .setMostSigBits(UUID.randomUUID().getMostSignificantBits())
        .setLeastSigBits(UUID.randomUUID().getLeastSignificantBits())
        .build();
    jobworkerDetailsProto =
        MockJobworkerDetails.createJobworkerDetails(
            ProtobufUtils.fromProtobuf(jwUuid).toString()).getProtoBufMessage();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (ozoneManager != null) {
      ozoneManager.close();
    }
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testRegisterJobworker() throws IOException {
    ExtendedJobWorkDetailsProto extendedJobWorkDetailsProto =
        ExtendedJobWorkDetailsProto.newBuilder()
            .setJobworkerDetails(jobworkerDetailsProto)
            .build();
    RegisterJobworkerRequest request =
        RegisterJobworkerRequest.newBuilder()
            .setExtendedJobWorkDetailsProto(extendedJobWorkDetailsProto)
            .build();

    RegisterJobworkerResponse response = client.register(request);

    assertNotNull(response);
    assertEquals(RegisterJobworkerResponse.ReturnCode.SUCCESS, response.getReturnCode());
    assertEquals(jwUuid, response.getJobworkerUUID());
    assertNotNull(response.getHostname());
    assertNotNull(response.getIpAddress());
    assertNotNull(response.getNetworkLocation());
    assertNotNull(response.getNetworkName());
    assertEquals(ozoneManager.getOmStorage().getClusterID(), response.getClusterID());
    assertEquals(ozoneManager.getNodeDetails().getServiceId(), response.getOmServiceId());
  }

  @Test
  public void testRegisterAndSendHeartbeat() throws IOException {
    registerJobworker();
    // Send heartbeat after registration
    JobworkerServiceProtocolProtos.SendHeartbeatRequest heartbeatRequest = createHeartbeatRequest();
    JobworkerServiceProtocolProtos.SendHeartbeatResponseProto heartbeatResponse =
        client.sendHeartbeat(heartbeatRequest);
    // Verify heartbeat response
    assertNotNull(heartbeatResponse);
    assertEquals(jwUuid, heartbeatResponse.getJobworkerUUID());
  }

  @Test
  public void testSendHeartbeatReregister() throws IOException {
    // Send heartbeat without registering first
    JobworkerServiceProtocolProtos.SendHeartbeatRequest heartbeatRequest = createHeartbeatRequest();
    JobworkerServiceProtocolProtos.SendHeartbeatResponseProto heartbeatResponse =
        client.sendHeartbeat(heartbeatRequest);

    // Verify heartbeat response contains a reregister command
    assertNotNull(heartbeatResponse);
    assertEquals(jwUuid, heartbeatResponse.getJobworkerUUID());
    assertTrue(heartbeatResponse.getCommandsCount() > 0,
        "Reregister command expected for heartbeat from unregistered worker");

    List<JobworkerServiceProtocolProtos.OMJobworkerCommandProto> commands = heartbeatResponse.getCommandsList();
    boolean hasReregisterCommand = commands.stream()
        .anyMatch(cmd ->
            cmd.getCommandType() == JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type.reregisterCommand);
    assertTrue(hasReregisterCommand, "Response should contain a reregister command");
  }

  private void registerJobworker() throws IOException {
    ExtendedJobWorkDetailsProto extendedJobWorkDetailsProto =
        ExtendedJobWorkDetailsProto.newBuilder()
            .setJobworkerDetails(jobworkerDetailsProto)
            .build();
    RegisterJobworkerRequest request =
        RegisterJobworkerRequest.newBuilder()
            .setExtendedJobWorkDetailsProto(extendedJobWorkDetailsProto)
            .build();

    client.register(request);
  }

  private JobworkerServiceProtocolProtos.SendHeartbeatRequest createHeartbeatRequest() {
    return JobworkerServiceProtocolProtos.SendHeartbeatRequest.newBuilder()
        .setJobworkerDetails(jobworkerDetailsProto)
        .setOmServiceId(omServiceId)
        .build();
  }

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("ozone.om.jobworker.service.enabled", true);
    conf.setInt("ozone.om.jobworker.grpc.port", 0);
    File newFolder = folder.toFile();
    if (!newFolder.exists()) {
      Assertions.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return conf;
  }
}

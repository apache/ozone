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

package org.apache.hadoop.ozone.container.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.impl.TestHddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.GenericTestUtils;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsUtils.isReadOnly;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.SUCCESS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getCreateContainerRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getTestBlockID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getTestContainerID;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newDeleteBlockRequestBuilder;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newDeleteChunkRequestBuilder;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newGetBlockRequestBuilder;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newGetCommittedBlockLengthBuilder;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newPutBlockRequestBuilder;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newReadChunkRequestBuilder;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.newWriteChunkRequestBuilder;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ratis.rpc.RpcType;

import static org.apache.ratis.rpc.SupportedRpcType.GRPC;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.junit.After;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test Container servers when security is enabled.
 */
public class TestSecureContainerServer {
  private static final String TEST_DIR
      = GenericTestUtils.getTestDir("dfs").getAbsolutePath() + File.separator;
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static CertificateClientTestImpl caClient;
  private static OzoneBlockTokenSecretManager blockTokenSecretManager;
  private static ContainerTokenSecretManager containerTokenSecretManager;

  @BeforeClass
  public static void setup() throws Exception {
    DefaultMetricsSystem.setMiniClusterMode(true);
    CONF.set(HddsConfigKeys.HDDS_METADATA_DIR_NAME, TEST_DIR);
    CONF.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    CONF.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    caClient = new CertificateClientTestImpl(CONF);

    String certSerialId =
        caClient.getCertificate().getSerialNumber().toString();
    SecurityConfig secConf = new SecurityConfig(CONF);
    long tokenLifetime = TimeUnit.HOURS.toMillis(1);

    blockTokenSecretManager = new OzoneBlockTokenSecretManager(
        secConf, tokenLifetime, certSerialId);
    blockTokenSecretManager.start(caClient);

    containerTokenSecretManager = new ContainerTokenSecretManager(
        secConf, tokenLifetime, certSerialId);
    containerTokenSecretManager.start(caClient);
  }

  @After
  public void cleanUp() throws IOException {
    containerTokenSecretManager.stop();
    blockTokenSecretManager.stop();
    FileUtils.deleteQuietly(new File(CONF.get(HDDS_DATANODE_DIR_KEY)));
  }

  @Test
  public void testClientServer() throws Exception {
    DatanodeDetails dd = MockDatanodeDetails.randomDatanodeDetails();
    HddsDispatcher hddsDispatcher = createDispatcher(dd,
        UUID.randomUUID(), CONF);
    runTestClientServer(1, (pipeline, conf) -> conf
            .setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
                pipeline.getFirstNode()
                    .getPort(DatanodeDetails.Port.Name.STANDALONE).getValue()),
        XceiverClientGrpc::new,
        (dn, conf) -> new XceiverServerGrpc(dd, conf,
            hddsDispatcher, caClient), (dn, p) -> {}, (p) -> {});
  }

  private static HddsDispatcher createDispatcher(DatanodeDetails dd, UUID scmId,
      OzoneConfiguration conf) throws IOException {
    ContainerSet containerSet = new ContainerSet();
    conf.set(HDDS_DATANODE_DIR_KEY,
        Paths.get(TEST_DIR, "dfs", "data", "hdds",
            RandomStringUtils.randomAlphabetic(4)).toString());
    VolumeSet volumeSet = new MutableVolumeSet(dd.getUuidString(), conf, null);
    DatanodeStateMachine stateMachine = Mockito.mock(
        DatanodeStateMachine.class);
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(dd);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    Map<ContainerProtos.ContainerType, Handler> handlers = Maps.newHashMap();
    for (ContainerProtos.ContainerType containerType :
        ContainerProtos.ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(containerType, conf,
              dd.getUuid().toString(),
              containerSet, volumeSet, metrics,
              TestHddsDispatcher.NO_OP_ICR_SENDER));
    }
    HddsDispatcher hddsDispatcher = new HddsDispatcher(
        conf, containerSet, volumeSet, handlers, context, metrics,
        TokenVerifier.create(new SecurityConfig((conf)), caClient));
    hddsDispatcher.setClusterId(scmId.toString());
    return hddsDispatcher;
  }

  @FunctionalInterface
  interface CheckedBiFunction<LEFT, RIGHT, OUT, THROWABLE extends Throwable> {
    OUT apply(LEFT left, RIGHT right) throws THROWABLE;
  }

  @Test
  public void testClientServerRatisGrpc() throws Exception {
    runTestClientServerRatis(GRPC, 1);
    runTestClientServerRatis(GRPC, 3);
  }

  static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails dn, OzoneConfiguration conf) throws IOException {
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT,
        dn.getPort(DatanodeDetails.Port.Name.RATIS).getValue());
    final String dir = TEST_DIR + dn.getUuid();
    conf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);
    final ContainerDispatcher dispatcher = createDispatcher(dn,
        UUID.randomUUID(), conf);
    return XceiverServerRatis.newXceiverServerRatis(dn, conf, dispatcher,
        new ContainerController(new ContainerSet(), Maps.newHashMap()),
        caClient, null);
  }

  private static void runTestClientServerRatis(RpcType rpc, int numNodes)
      throws Exception {
    runTestClientServer(numNodes,
        (pipeline, conf) -> RatisTestHelper.initRatisConf(rpc, conf),
        XceiverClientRatis::newXceiverClientRatis,
        TestSecureContainerServer::newXceiverServerRatis,
        (dn, p) -> RatisTestHelper.initXceiverServerRatis(rpc, dn, p),
        (p) -> {});
  }

  private static void runTestClientServer(
      int numDatanodes,
      CheckedBiConsumer<Pipeline, OzoneConfiguration, IOException> initConf,
      CheckedBiFunction<Pipeline, OzoneConfiguration, XceiverClientSpi,
          IOException> createClient,
      CheckedBiFunction<DatanodeDetails, OzoneConfiguration, XceiverServerSpi,
          IOException> createServer,
      CheckedBiConsumer<DatanodeDetails, Pipeline, IOException> initServer,
      Consumer<Pipeline> stopServer)
      throws Exception {
    final List<XceiverServerSpi> servers = new ArrayList<>();
    final Pipeline pipeline =
        MockPipeline.createPipeline(numDatanodes);

    initConf.accept(pipeline, CONF);

    for (DatanodeDetails dn : pipeline.getNodes()) {
      final XceiverServerSpi s = createServer.apply(dn, CONF);
      servers.add(s);
      s.start();
      initServer.accept(dn, pipeline);
    }

    try (XceiverClientSpi client = createClient.apply(pipeline, CONF)) {
      client.connect();

      long containerID = getTestContainerID();
      BlockID blockID = getTestBlockID(containerID);

      assertFailsTokenVerification(client,
          getCreateContainerRequest(containerID, pipeline));

      //create the container
      ContainerProtocolCalls.createContainer(client, containerID,
          getToken(ContainerID.valueOf(containerID)));

      Token<OzoneBlockTokenIdentifier> token =
          blockTokenSecretManager.generateToken(blockID,
              EnumSet.allOf(AccessModeProto.class), RandomUtils.nextLong());
      String encodedToken = token.encodeToUrlString();

      ContainerCommandRequestProto.Builder writeChunk =
          newWriteChunkRequestBuilder(pipeline, blockID, 1024, 0);
      assertRequiresToken(client, encodedToken, writeChunk);

      ContainerCommandRequestProto.Builder putBlock =
          newPutBlockRequestBuilder(pipeline, writeChunk.getWriteChunk());
      assertRequiresToken(client, encodedToken, putBlock);

      ContainerCommandRequestProto.Builder readChunk =
          newReadChunkRequestBuilder(pipeline, writeChunk.getWriteChunk());
      assertRequiresToken(client, encodedToken, readChunk);

      ContainerCommandRequestProto.Builder getBlock =
          newGetBlockRequestBuilder(pipeline, putBlock.getPutBlock());
      assertRequiresToken(client, encodedToken, getBlock);

      ContainerCommandRequestProto.Builder getCommittedBlockLength =
          newGetCommittedBlockLengthBuilder(pipeline, putBlock.getPutBlock());
      assertRequiresToken(client, encodedToken, getCommittedBlockLength);

      ContainerCommandRequestProto.Builder deleteChunk =
          newDeleteChunkRequestBuilder(pipeline, writeChunk.getWriteChunk());
      assertRequiresToken(client, encodedToken, deleteChunk);

      ContainerCommandRequestProto.Builder deleteBlock =
          newDeleteBlockRequestBuilder(pipeline, putBlock.getPutBlock());
      assertRequiresToken(client, encodedToken, deleteBlock);
    } finally {
      stopServer.accept(pipeline);
      servers.forEach(XceiverServerSpi::stop);
    }
  }

  private static void assertRequiresToken(XceiverClientSpi client,
      String encodedToken, ContainerCommandRequestProto.Builder requestBuilder)
      throws Exception {

    requestBuilder.setEncodedToken("");
    assertFailsTokenVerification(client, requestBuilder.build());

    requestBuilder.setEncodedToken(encodedToken);
    assertSucceeds(client, requestBuilder.build());
  }

  private static void assertSucceeds(
      XceiverClientSpi client, ContainerCommandRequestProto req)
      throws IOException {
    ContainerCommandResponseProto response = client.sendCommand(req);
    assertEquals(SUCCESS, response.getResult());
  }

  private static void assertFailsTokenVerification(XceiverClientSpi client,
      ContainerCommandRequestProto request) throws Exception {
    if (client instanceof XceiverClientGrpc || isReadOnly(request)) {
      ContainerCommandResponseProto response = client.sendCommand(request);
      assertNotEquals(response.getResult(), ContainerProtos.Result.SUCCESS);
      String msg = response.getMessage();
      assertTrue(msg, msg.contains("token verification failed"));
    } else {
      assertRootCauseMessage("token verification failed",
          Assert.assertThrows(IOException.class, () ->
              client.sendCommand(request)));
    }
  }

  private static void assertRootCauseMessage(String contained, Throwable t) {
    assertNotNull(t);
    Throwable rootCause = ExceptionUtils.getRootCause(t);
    assertNotNull(rootCause);
    String msg = rootCause.getMessage();
    assertTrue(msg, msg.contains(contained));
  }

  private static String getToken(ContainerID containerID) throws IOException {
    String username = "";
    return containerTokenSecretManager.generateToken(
        containerTokenSecretManager.createIdentifier(username, containerID)
    ).encodeToUrlString();
  }
}
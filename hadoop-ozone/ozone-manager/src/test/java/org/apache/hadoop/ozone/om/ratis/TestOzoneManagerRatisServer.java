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

package org.apache.hadoop.ozone.om.ratis;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.security.OMCertificateClient;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test OM Ratis server.
 */
public class TestOzoneManagerRatisServer {
  @TempDir
  private Path folder;

  private OzoneConfiguration conf;
  private OzoneManagerRatisServer omRatisServer;
  private String clientId = UUID.randomUUID().toString();
  private static final long RATIS_RPC_TIMEOUT = 500L;
  private OMMetadataManager omMetadataManager;
  private OzoneManager ozoneManager;
  private OMNodeDetails omNodeDetails;
  private SecurityConfig secConfig;
  private OMCertificateClient certClient;

  @BeforeAll
  public static void setup() {
    ExitUtils.disableSystemExit();
  }
  
  @BeforeEach
  public void init(@TempDir Path metaDirPath) throws Exception {
    conf = new OzoneConfiguration();
    String omID = UUID.randomUUID().toString();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    conf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
        RATIS_RPC_TIMEOUT, TimeUnit.MILLISECONDS);
    int ratisPort = conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_PORT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT);
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    omNodeDetails = new OMNodeDetails.Builder()
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setOMNodeId(omID)
        .setOMServiceId(OzoneConsts.OM_SERVICE_ID_DEFAULT)
        .build();
    OMStorage omStorage = mock(OMStorage.class);
    when(omStorage.getOmCertSerialId()).thenReturn(null);
    when(omStorage.getClusterID()).thenReturn("test");
    when(omStorage.getOmId()).thenReturn(UUID.randomUUID().toString());
    // Starts a single node Ratis server
    ozoneManager = mock(OzoneManager.class);
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getTransactionInfo()).thenReturn(TransactionInfo.DEFAULT_VALUE);
    when(ozoneManager.getConfiguration()).thenReturn(conf);
    final OmConfig omConfig = conf.getObject(OmConfig.class);
    when(ozoneManager.getConfig()).thenReturn(omConfig);
    secConfig = new SecurityConfig(conf);
    HddsProtos.OzoneManagerDetailsProto omInfo =
        OzoneManager.getOmDetailsProto(conf, omID);
    certClient =
        new OMCertificateClient(
            secConfig, null, omStorage, omInfo, "", null, null, null);
    omRatisServer = OzoneManagerRatisServer.newOMRatisServer(conf, ozoneManager,
      omNodeDetails, Collections.emptyMap(), secConfig, certClient, false);
    omRatisServer.start();
  }

  @AfterEach
  public void shutdown() throws IOException {
    if (omRatisServer != null) {
      omRatisServer.stop();
    }
    certClient.close();
  }

  /**
   * Start a OM Ratis Server and checks its state.
   */
  @Test
  public void testStartOMRatisServer() throws Exception {
    assertEquals(LifeCycle.State.RUNNING,
        omRatisServer.getServerState(),
        "Ratis Server should be in running state");
  }

  @Test
  public void testLoadSnapshotInfoOnStart() throws Exception {
    // Stop the Ratis server and manually update the snapshotInfo.
    omRatisServer.getOmStateMachine().loadSnapshotInfoFromDB();
    omRatisServer.stop();

    SnapshotInfo snapshotInfo =
        omRatisServer.getOmStateMachine().getLatestSnapshot();

    TermIndex newSnapshotIndex = TermIndex.valueOf(
        snapshotInfo.getTerm(), snapshotInfo.getIndex() + 100);

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(newSnapshotIndex));

    // Start new Ratis server. It should pick up and load the new SnapshotInfo
    omRatisServer = OzoneManagerRatisServer.newOMRatisServer(conf, ozoneManager,
        omNodeDetails, Collections.emptyMap(), secConfig, certClient, false);
    omRatisServer.start();
    TermIndex lastAppliedTermIndex =
        omRatisServer.getLastAppliedTermIndex();

    assertEquals(newSnapshotIndex.getIndex(), lastAppliedTermIndex.getIndex());
    assertEquals(newSnapshotIndex.getTerm(), lastAppliedTermIndex.getTerm());
  }

  /**
   * Test that all of {@link OzoneManagerProtocolProtos.Type} enum values are
   * categorized in {@link OmUtils#isReadOnly(OMRequest)}.
   */
  @Test
  public void testIsReadOnlyCapturesAllCmdTypeEnums() throws Exception {
    LogCapturer logCapturer = LogCapturer.captureLogs(OmUtils.class);
    OzoneManagerProtocolProtos.Type[] cmdTypes =
        OzoneManagerProtocolProtos.Type.values();

    for (OzoneManagerProtocolProtos.Type cmdtype : cmdTypes) {
      OMRequest request = OMRequest.newBuilder()
          .setCmdType(cmdtype)
          .setClientId(clientId)
          .build();
      OmUtils.isReadOnly(request);
      assertThat(logCapturer.getOutput())
          .withFailMessage(cmdtype + " is not categorized in OmUtils#isReadyOnly")
          .doesNotContain("CmdType " + cmdtype + " is not categorized as readOnly or not.");
      logCapturer.clearOutput();
    }
  }

  @Test
  public void verifyRaftGroupIdGenerationWithDefaultOmServiceId() throws
      Exception {
    UUID uuid = UUID.nameUUIDFromBytes(OzoneConsts.OM_SERVICE_ID_DEFAULT
        .getBytes(UTF_8));
    RaftGroupId raftGroupId = omRatisServer.getRaftGroup().getGroupId();
    assertEquals(uuid, raftGroupId.getUuid());
    assertEquals(raftGroupId.toByteString().size(), 16);
  }

  @Test
  public void verifyRaftGroupIdGenerationWithCustomOmServiceId(@TempDir Path metaDirPath) throws
      Exception {
    String customOmServiceId = "omSIdCustom123";
    OzoneConfiguration newConf = new OzoneConfiguration();
    String newOmId = UUID.randomUUID().toString();
    newConf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    newConf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
        RATIS_RPC_TIMEOUT, TimeUnit.MILLISECONDS);
    int ratisPort = 9873;
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setOMNodeId(newOmId)
        .setOMServiceId(customOmServiceId)
        .build();
    // Starts a single node Ratis server
    omRatisServer.stop();
    OzoneManagerRatisServer newOmRatisServer = OzoneManagerRatisServer
        .newOMRatisServer(newConf, ozoneManager, nodeDetails,
            Collections.emptyMap(), secConfig, certClient, false);
    newOmRatisServer.start();

    UUID uuid = UUID.nameUUIDFromBytes(customOmServiceId.getBytes(UTF_8));
    RaftGroupId raftGroupId = newOmRatisServer.getRaftGroup().getGroupId();
    assertEquals(uuid, raftGroupId.getUuid());
    assertEquals(raftGroupId.toByteString().size(), 16);
    newOmRatisServer.stop();
  }
}

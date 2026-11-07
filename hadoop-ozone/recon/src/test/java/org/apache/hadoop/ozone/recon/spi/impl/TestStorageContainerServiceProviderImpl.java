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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_ENABLED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HTTP_POLICY_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.ha.InterSCMGrpcClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationInternalInterface;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.security.ReconCertificateClient;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

/**
 * Class to test StorageContainerServiceProviderImpl APIs.
 */
public class TestStorageContainerServiceProviderImpl {

  private static final String LEADER = "scm1.example.com:9894:LEADER:e428ca07-b2a3-4756-bf9b-a4abb033c7d1:10.0.0.1";

  private Injector injector;
  private HddsProtos.PipelineID pipelineID;
  private File testDir;

  @BeforeEach
  void setup(@TempDir File tempDir) {
    testDir = tempDir;
    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        try {
          StorageContainerLocationInternalInterface mockScmClient = mock(
              StorageContainerLocationInternalInterface.class);
          ReconUtils reconUtils = spy(new ReconUtils());
          OzoneConfiguration conf = new OzoneConfiguration();
          conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
          pipelineID = PipelineID.randomId().getProtobuf();
          when(mockScmClient.getPipeline(pipelineID))
              .thenReturn(mock(Pipeline.class));
          bind(StorageContainerLocationInternalInterface.class)
              .toInstance(mockScmClient);
          bind(StorageContainerServiceProvider.class)
              .to(StorageContainerServiceProviderImpl.class);
          bind(OzoneConfiguration.class).
              toInstance(conf);
          bind(ReconUtils.class).toInstance(reconUtils);
        } catch (Exception e) {
          fail();
        }
      }
    });
  }

  @Test
  public void testGetPipelines() throws IOException {
    StorageContainerServiceProvider scmProvider =
        injector.getInstance(StorageContainerServiceProvider.class);
    StorageContainerLocationInternalInterface scmClient =
        injector.getInstance(StorageContainerLocationInternalInterface.class);
    scmProvider.getPipelines();
    verify(scmClient, times(1)).listPipelines();
  }

  @Test
  void testGetContainerInfoUnwrapsMissingContainer() throws IOException {
    StorageContainerServiceProvider scmProvider = injector.getInstance(StorageContainerServiceProvider.class);
    StorageContainerLocationInternalInterface scmClient =
        injector.getInstance(StorageContainerLocationInternalInterface.class);
    when(scmClient.getContainer(1L)).thenThrow(
        new RemoteException(ContainerNotFoundException.class.getName(), "Container 1 not found"));

    assertThrows(ContainerNotFoundException.class, () -> scmProvider.getContainerInfo(1L));
  }

  @Test
  public void testGetPipeline() throws IOException {
    StorageContainerServiceProvider scmProvider =
        injector.getInstance(StorageContainerServiceProvider.class);
    StorageContainerLocationInternalInterface scmClient =
        injector.getInstance(StorageContainerLocationInternalInterface.class);
    Pipeline pipeline = scmProvider.getPipeline(pipelineID);
    assertNotNull(pipeline);
    verify(scmClient, times(1))
        .getPipeline(pipelineID);
  }

  /**
   * Recon downloads the SCM snapshot from whichever peer reports itself as
   * LEADER, using the resolved address carried in the encoded role string.
   */
  @Test
  public void testGetSCMDBSnapshotConnectsToLeader() throws Exception {
    StorageContainerServiceProvider scmProvider =
        injector.getInstance(StorageContainerServiceProvider.class);
    StorageContainerLocationInternalInterface scmClient =
        injector.getInstance(StorageContainerLocationInternalInterface.class);

    setPeerRoles(scmClient,
        "[2001:db8::2]:9894:FOLLOWER:61b1c8e5-da40-4567-8a17-96a0234ba14e:[2001:db8:0:0:0:0:0:2]",
        "[2001:db8::1]:9894:LEADER:e428ca07-b2a3-4756-bf9b-a4abb033c7d1:[2001:db8:0:0:0:0:0:1]");

    AtomicReference<String> downloadHost = new AtomicReference<>();
    try (MockedStatic<HddsServerUtil> stubbedServerUtil = mockStatic(HddsServerUtil.class);
         MockedConstruction<ReconCertificateClient> stubbedCertClient =
             mockConstruction(ReconCertificateClient.class);
         MockedConstruction<InterSCMGrpcClient> downloaders =
             mockConstruction(InterSCMGrpcClient.class, (downloader, context) -> {
               downloadHost.set((String) context.arguments().get(0));
               when(downloader.download(any()))
                   .thenReturn(CompletableFuture.completedFuture(null));
             })) {
      scmProvider.getSCMDBSnapshot();

      assertEquals(1, downloaders.constructed().size());
    }

    // The follower is listed first, so this also pins that the LEADER entry is
    // the one selected. The resolved literal is handed over verbatim, unbracketed.
    assertEquals("2001:db8:0:0:0:0:0:1", downloadHost.get());
    // Without gRPC TLS, the inter-SCM gRPC server accepts Recon, so HTTP is not used.
    verify(injector.getInstance(ReconUtils.class), never()).makeHttpCall(any(), anyString(), anyBoolean());
  }

  /**
   * With gRPC TLS, Recon downloads the SCM snapshot from the leader over HTTP from the SCM DB checkpoint endpoint.
   */
  @Test
  public void testGetSCMDBSnapshotOverHttpConnectsToLeader() throws Exception {
    String url = getSCMDBSnapshot(false,
        "[2001:db8::2]:9894:FOLLOWER:61b1c8e5-da40-4567-8a17-96a0234ba14e:[2001:db8:0:0:0:0:0:2]",
        "[2001:db8::1]:9894:LEADER:e428ca07-b2a3-4756-bf9b-a4abb033c7d1:[2001:db8:0:0:0:0:0:1]");

    // The follower is listed first, so this also pins that the LEADER entry is
    // the one selected.
    assertEquals("http://[2001:db8::1]:9876/dbCheckpoint?flushBeforeCheckpoint=true", url);
  }

  /**
   * With SCM HA, Recon uses the HTTP address of the leader SCM node, and SPNEGO when SCM requires Kerberos.
   * The leader node is matched by host and Ratis port, since SCMs may share a host.
   */
  @Test
  public void testGetSCMDBSnapshotUsesLeaderHttpAddress() throws Exception {
    OzoneConfiguration conf = injector.getInstance(OzoneConfiguration.class);
    conf.set(OZONE_SCM_SERVICE_IDS_KEY, "scmservice");
    conf.set(OZONE_SCM_NODES_KEY + ".scmservice", "scm1,scm2");
    conf.set(OZONE_SCM_ADDRESS_KEY + ".scmservice.scm1", "scm.example.com");
    conf.set(OZONE_SCM_ADDRESS_KEY + ".scmservice.scm2", "scm.example.com:9895");
    conf.set(OZONE_SCM_HTTP_ADDRESS_KEY + ".scmservice.scm1", "scm.example.com:9998");
    conf.set(OZONE_SCM_HTTP_ADDRESS_KEY + ".scmservice.scm2", "scm.example.com:9999");
    conf.set(HDDS_SCM_HTTP_AUTH_TYPE, "kerberos");

    String url = getSCMDBSnapshot(true,
        "scm.example.com:9894:FOLLOWER:61b1c8e5-da40-4567-8a17-96a0234ba14e:10.0.0.1",
        "scm.example.com:9895:LEADER:e428ca07-b2a3-4756-bf9b-a4abb033c7d1:10.0.0.1");

    assertEquals("http://scm.example.com:9999/dbCheckpoint?flushBeforeCheckpoint=true", url);
  }

  /**
   * In secure mode with gRPC TLS enabled, Recon still downloads the SCM DB over plain HTTP, but warns that it is
   * not encrypted.
   */
  @Test
  public void testGetSCMDBSnapshotWarnsOverHttpWithGrpcTls() throws Exception {
    try (LogCapturer logs = LogCapturer.captureLogs(StorageContainerServiceProviderImpl.class)) {
      assertEquals("http://scm1.example.com:9876/dbCheckpoint?flushBeforeCheckpoint=true",
          getSCMDBSnapshot(false, LEADER));
      assertThat(logs.getOutput()).contains("Downloading the SCM DB unencrypted over HTTP");
    }
  }

  /**
   * Recon downloads the SCM DB from the HTTPS address when HTTPS is enabled.
   */
  @Test
  public void testGetSCMDBSnapshotOverHttps() throws Exception {
    OzoneConfiguration conf = injector.getInstance(OzoneConfiguration.class);
    conf.set(OZONE_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    try (LogCapturer logs = LogCapturer.captureLogs(StorageContainerServiceProviderImpl.class)) {
      assertEquals("https://scm1.example.com:9877/dbCheckpoint?flushBeforeCheckpoint=true",
          getSCMDBSnapshot(false, LEADER));
      assertThat(logs.getOutput()).doesNotContain("unencrypted");
    }
  }

  /**
   * A checkpoint without the completion flag, e.g. cut short by an error on SCM, is rejected.
   */
  @Test
  public void testGetSCMDBSnapshotRejectsIncompleteCheckpoint() throws Exception {
    enableGrpcTls();
    ReconUtils reconUtils = injector.getInstance(ReconUtils.class);
    setPeerRoles(injector.getInstance(StorageContainerLocationInternalInterface.class), LEADER);
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
    doReturn(connection).when(reconUtils).makeHttpCall(any(), anyString(), anyBoolean());
    doNothing().when(reconUtils).untarCheckpointFile(any(), any());

    assertNull(injector.getInstance(StorageContainerServiceProvider.class).getSCMDBSnapshot());
    assertThat(injector.getInstance(ReconContext.class).getErrors())
        .contains(ReconContext.ErrorCode.GET_SCM_DB_SNAPSHOT_FAILED);
  }

  /**
   * Cancelling the SCM DB sync returns without waiting for a download that SCM stalls, either after sending part of
   * the checkpoint or before responding, and the partial checkpoint is removed.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetSCMDBSnapshotCancelDoesNotWaitForStalledDownload(boolean stallMidBody) throws Exception {
    enableGrpcTls();
    CountDownLatch responding = new CountDownLatch(1);
    CountDownLatch stall = new CountDownLatch(1);
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(OZONE_DB_CHECKPOINT_HTTP_ENDPOINT, exchange -> {
      if (stallMidBody) {
        exchange.sendResponseHeaders(200, 1 << 20);
        exchange.getResponseBody().write(new byte[1024]);
        exchange.getResponseBody().flush();
      }
      responding.countDown();
      Uninterruptibles.awaitUninterruptibly(stall);
      if (!stallMidBody) {
        exchange.sendResponseHeaders(200, -1);
      }
      exchange.close();
    });
    server.start();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      injector.getInstance(OzoneConfiguration.class)
          .set(OZONE_SCM_HTTP_ADDRESS_KEY, "127.0.0.1:" + server.getAddress().getPort());
      setPeerRoles(injector.getInstance(StorageContainerLocationInternalInterface.class),
          "127.0.0.1:9894:LEADER:e428ca07-b2a3-4756-bf9b-a4abb033c7d1:127.0.0.1");
      Future<DBCheckpoint> sync =
          executor.submit(() -> injector.getInstance(StorageContainerServiceProvider.class).getSCMDBSnapshot());
      assertTrue(responding.await(10, TimeUnit.SECONDS));
      if (stallMidBody) {
        GenericTestUtils.waitFor(() -> listTarFiles().stream().anyMatch(f -> f.length() > 0), 100, 10000);
      }

      // Like ReconStorageContainerManagerFacade#cancelScmDbSnapshotSync
      sync.cancel(true);
      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
      stall.countDown();
    }
    try {
      // The download thread cleans up when it stops.
      GenericTestUtils.waitFor(() -> Thread.getAllStackTraces().keySet().stream()
          .noneMatch(thread -> thread.getName().startsWith("ReconSCMDBDownload")), 100, 10000);
      assertThat(listTarFiles()).isEmpty();
    } finally {
      server.stop(0);
    }
  }

  private Collection<File> listTarFiles() {
    return FileUtils.listFiles(testDir, new String[] {"tar"}, true);
  }

  /**
   * Gets the SCM DB snapshot from SCM peers with {@code peerRoles}, in secure mode with gRPC TLS enabled.
   *
   * @return the URL the snapshot was downloaded from
   */
  private String getSCMDBSnapshot(boolean spnegoEnabled, String... peerRoles) throws Exception {
    StorageContainerServiceProvider scmProvider =
        injector.getInstance(StorageContainerServiceProvider.class);
    StorageContainerLocationInternalInterface scmClient =
        injector.getInstance(StorageContainerLocationInternalInterface.class);
    ReconUtils reconUtils = injector.getInstance(ReconUtils.class);

    enableGrpcTls();
    setPeerRoles(scmClient, peerRoles);
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
    doReturn(connection).when(reconUtils).makeHttpCall(any(), anyString(), anyBoolean());
    doAnswer(invocation -> {
      Path untarredDbDir = invocation.getArgument(1);
      Files.createDirectories(untarredDbDir);
      Files.createFile(untarredDbDir.resolve(OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME));
      return null;
    }).when(reconUtils).untarCheckpointFile(any(), any());

    assertNotNull(scmProvider.getSCMDBSnapshot());

    ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
    verify(reconUtils).makeHttpCall(any(), url.capture(), eq(spnegoEnabled));
    return url.getValue();
  }

  private void enableGrpcTls() {
    OzoneConfiguration conf = injector.getInstance(OzoneConfiguration.class);
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(HDDS_GRPC_TLS_ENABLED, true);
  }

  private static void setPeerRoles(StorageContainerLocationInternalInterface scmClient, String... peerRoles)
      throws IOException {
    when(scmClient.getScmInfo()).thenReturn(new ScmInfo.Builder()
        .setClusterId("CID-6a1b5b1e-3f5a-4b5c-8f6d-2a1c3e4f5a6b")
        .setScmId("scm1")
        .setPeerRoles(Arrays.asList(peerRoles))
        .build());
  }
}

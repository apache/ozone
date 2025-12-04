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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_TEST_CERT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DEFAULT_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.container.ContainerID.valueOf;
import static org.apache.hadoop.hdds.scm.pipeline.MockPipeline.createPipeline;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getCloseContainer;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getCreateContainerSecureRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getTestContainerID;
import static org.apache.hadoop.ozone.container.common.helpers.TokenHelper.encode;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.apache.ozone.test.GenericTestUtils.LogCapturer.captureLogs;
import static org.apache.ozone.test.GenericTestUtils.setLogLevel;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientManager.ScmClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.token.ContainerTokenIdentifier;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.SecretKeyTestClient;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.TokenHelper;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.replication.SimpleContainerDownloader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.event.Level;

/**
 * Tests ozone containers via secure grpc/netty.
 */
public class TestOzoneContainerWithTLS {

  private static final int CERT_LIFETIME = 10; // seconds
  private static final int ROOT_CERT_LIFE_TIME = 20; // seconds

  private String clusterID;
  private OzoneConfiguration conf;
  private DatanodeDetails dn;
  private Pipeline pipeline;
  private CertificateClientTestImpl caClient;
  private SecretKeyClient keyClient;
  private ContainerTokenSecretManager secretManager;

  @TempDir
  private Path tempFolder;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();

    conf.set(OZONE_METADATA_DIRS,
        tempFolder.resolve("meta").toString());
    conf.set(HDDS_DATANODE_DIR_KEY,
        tempFolder.resolve("data").toString());
    conf.set(HDDS_DATANODE_CONTAINER_DB_DIR,
        tempFolder.resolve("containers").toString());

    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(HDDS_GRPC_TLS_ENABLED, true);
    conf.setBoolean(HDDS_GRPC_TLS_TEST_CERT, true);
    conf.setBoolean(HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED, false);
    conf.set(HDDS_X509_DEFAULT_DURATION, ofSeconds(CERT_LIFETIME).toString());
    conf.set(HddsConfigKeys.HDDS_X509_MAX_DURATION,
        ofSeconds(ROOT_CERT_LIFE_TIME).toString());
    conf.set(HDDS_X509_RENEW_GRACE_DURATION, ofSeconds(2).toString());
    conf.setInt(HDDS_BLOCK_TOKEN_EXPIRY_TIME, 1000);

    clusterID = UUID.randomUUID().toString();
    caClient = new CertificateClientTestImpl(conf, false);
    keyClient = new SecretKeyTestClient();
    secretManager = new ContainerTokenSecretManager(1000, keyClient);

    dn = aDatanode();
    pipeline = createPipeline(singletonList(dn));

    setLogLevel(ClientTrustManager.class, Level.DEBUG);
  }

  @Test
  public void testCertificateLifetime() {
    Date atTimeAfterExpiry = Date.from(LocalDateTime.now()
        .plusSeconds(CERT_LIFETIME)
        .atZone(ZoneId.systemDefault())
        .toInstant());

    assertThrows(CertificateExpiredException.class,
        () -> caClient.getCertificate().checkValidity(atTimeAfterExpiry));
  }

  @ParameterizedTest(name = "Container token enabled: {0}")
  @ValueSource(booleans = {false, true})
  public void createContainer(boolean containerTokenEnabled)
      throws Exception {
    conf.setBoolean(HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED,
        containerTokenEnabled);
    OzoneContainer container = createAndStartOzoneContainerInstance();

    try (XceiverClientGrpc client =
             new XceiverClientGrpc(pipeline, conf, aClientTrustManager())) {
      client.connect();

      createContainer(client, containerTokenEnabled, getTestContainerID());
    } finally {
      if (container != null) {
        container.stop();
      }
    }
  }

  @ParameterizedTest(name = "Container token enabled: {0}")
  @ValueSource(booleans = {false, true})
  public void downloadContainer(boolean containerTokenEnabled)
      throws Exception {
    conf.setBoolean(HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED,
        containerTokenEnabled);
    OzoneContainer container = createAndStartOzoneContainerInstance();

    ScmClientConfig scmClientConf = conf.getObject(ScmClientConfig.class);
    XceiverClientManager clientManager =
        new XceiverClientManager(conf, scmClientConf, aClientTrustManager());
    XceiverClientSpi client = null;
    try {
      client = clientManager.acquireClient(pipeline);
      // at this point we have an established connection from the client to
      // the container, and we do not expect a new SSL handshake while we are
      // running container ops until the renewal, however it may happen, as
      // the protocol can do a renegotiation at any time, so this dynamic
      // introduces a very low chance of flakiness.
      // The downloader client when it connects first, will do a failing
      // handshake that we are expecting because before downloading, we wait
      // for the expiration without renewing the certificate.
      List<Long> containers = new ArrayList<>();
      List<DatanodeDetails> sourceDatanodes = new ArrayList<>();
      sourceDatanodes.add(dn);

      containers.add(createAndCloseContainer(client, containerTokenEnabled));
      letCertExpire();
      containers.add(createAndCloseContainer(client, containerTokenEnabled));
      assertDownloadContainerFails(containers.get(0), sourceDatanodes);

      caClient.renewKey();
      containers.add(createAndCloseContainer(client, containerTokenEnabled));
      assertDownloadContainerWorks(containers, sourceDatanodes);
    } finally {
      if (container != null) {
        container.stop();
      }
      if (client != null) {
        clientManager.releaseClient(client, true);
      }
      IOUtils.closeQuietly(clientManager);
    }
  }

  @ParameterizedTest(name = "Container token enabled: {0}")
  @ValueSource(booleans = {false, true})
  public void testDNContainerOperationClient(boolean containerTokenEnabled)
      throws Exception {
    conf.setBoolean(HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED,
        containerTokenEnabled);
    OzoneContainer container = createAndStartOzoneContainerInstance();
    ScmClientConfig scmClientConf = conf.getObject(ScmClientConfig.class);
    XceiverClientManager clientManager =
        new XceiverClientManager(conf, scmClientConf, aClientTrustManager());
    XceiverClientSpi client = null;
    try (DNContainerOperationClient dnClient =
             new DNContainerOperationClient(conf, caClient, keyClient)) {
      client = clientManager.acquireClient(pipeline);
      long containerId = createAndCloseContainer(client, containerTokenEnabled);
      dnClient.getContainerChecksumInfo(containerId, dn);
    } finally {
      if (container != null) {
        container.stop();
      }
      if (client != null) {
        clientManager.releaseClient(client, true);
      }
      IOUtils.closeQuietly(clientManager);
    }
  }

  @Test
  public void testGetContainerMerkleTree() throws IOException {
    conf.setBoolean(HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED, true);
    OzoneContainer container = createAndStartOzoneContainerInstance();
    ScmClientConfig scmClientConf = conf.getObject(ScmClientConfig.class);
    XceiverClientManager clientManager =
        new XceiverClientManager(conf, scmClientConf, aClientTrustManager());
    XceiverClientSpi client = null;
    try {
      client = clientManager.acquireClient(pipeline);
      long containerId = createAndCloseContainer(client, true);
      TokenHelper tokenHelper = new TokenHelper(new SecurityConfig(conf), keyClient);
      String containerToken = encode(tokenHelper.getContainerToken(
          ContainerID.valueOf(containerId)));
      ContainerProtos.GetContainerChecksumInfoResponseProto response =
          ContainerProtocolCalls.getContainerChecksumInfo(client,
              containerId, containerToken);
      // Getting container merkle tree with valid container token
      assertFalse(response.getContainerChecksumInfo().isEmpty());

      // Getting container merkle tree with invalid container token
      XceiverClientSpi finalClient = client;
      StorageContainerException exception = assertThrows(StorageContainerException.class,
          () -> ContainerProtocolCalls.getContainerChecksumInfo(
          finalClient, containerId, "invalidContainerToken"));
      assertEquals(ContainerProtos.Result.BLOCK_TOKEN_VERIFICATION_FAILED, exception.getResult());
    } finally {
      if (container != null) {
        container.stop();
      }
      if (client != null) {
        clientManager.releaseClient(client, true);
      }
      IOUtils.closeQuietly(clientManager);
    }
  }

  @Test
  public void testLongLivingClientWithCertRenews() throws Exception {
    LogCapturer logs = captureLogs(ClientTrustManager.class);
    OzoneContainer container = createAndStartOzoneContainerInstance();

    ScmClientConfig scmClientConf = conf.getObject(ScmClientConfig.class);
    scmClientConf.setStaleThreshold(500);
    XceiverClientManager clientManager =
        new XceiverClientManager(conf, scmClientConf, aClientTrustManager());
    assertClientTrustManagerLoading(true, logs, "Client loaded certificates.");

    XceiverClientSpi client = null;
    try {
      client = clientManager.acquireClient(pipeline);
      createAndCloseContainer(client, false);
      assertClientTrustManagerLoading(false, logs,
          "Check client did not reloaded certificates.");

      letCACertExpire();

      // works based off of initial SSL handshake
      createAndCloseContainer(client, false);
      assertClientTrustManagerLoading(false, logs,
          "Check client did not reloaded certificates.");

      clientManager.releaseClient(client, true);
      client = clientManager.acquireClient(pipeline);
      assertClientTrustManagerLoading(false, logs,
          "Check second client creation does not reload certificates.");
      // aim is to only load certs at first start, and in case of a verification
      // failure

      try {
        // fails after a new client is created, as we just have expired certs
        // in the source (caClient), so container ops still fail even though
        // a retry/reload happens, but we expect it to happen before the failure
        createAndCloseContainer(client, false);
      } catch (Throwable e) {
        assertClientTrustManagerFailedAndRetried(logs);
        while (e.getCause() != null) {
          e = e.getCause();
        }
        assertInstanceOf(CertificateExpiredException.class, e);
      } finally {
        clientManager.releaseClient(client, true);
      }

      client = clientManager.acquireClient(pipeline);
      caClient.renewRootCA();
      caClient.renewKey();
      Thread.sleep(1000); // wait for reloading trust managers to reload

      createAndCloseContainer(client, false);
      assertClientTrustManagerFailedAndRetried(logs);

    } finally {
      if (client != null) {
        clientManager.releaseClient(client, true);
      }
      if (container != null) {
        container.stop();
      }
      IOUtils.closeQuietly(clientManager);
    }
  }

  private void assertClientTrustManagerLoading(
      boolean happened, LogCapturer logs, String msg) {
    String loadedMsg = "Loading certificates for client.";
    assertEquals(happened, logs.getOutput().contains(loadedMsg), msg);
    logs.clearOutput();
  }

  private void assertClientTrustManagerFailedAndRetried(LogCapturer logs) {
    assertThat(logs.getOutput())
        .withFailMessage("Check client failed first, and initiates a reload.")
        .contains("trying to re-fetch rootCA");
    assertThat(logs.getOutput())
        .withFailMessage("Check client loaded certificates.")
        .contains("Loading certificates for client.");
    logs.clearOutput();
  }

  private OzoneContainer createAndStartOzoneContainerInstance() {
    OzoneContainer container = null;
    try {
      StateContext stateContext = ContainerTestUtils.getMockContext(dn, conf);
      VolumeChoosingPolicy volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);
      container = new OzoneContainer(
          null, dn, conf, stateContext, caClient, keyClient, volumeChoosingPolicy);
      MutableVolumeSet volumeSet = container.getVolumeSet();
      StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())
          .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempFolder.toFile()));
      ContainerTestUtils.initializeDatanodeLayout(conf, dn);
      container.start(clusterID);
    } catch (Throwable e) {
      if (container != null) {
        container.stop();
      }
      fail(e);
    }
    return container;
  }

  private void assertDownloadContainerFails(long containerId,
      List<DatanodeDetails> sourceDatanodes) {
    LogCapturer logCapture = captureLogs(SimpleContainerDownloader.class);
    SimpleContainerDownloader downloader =
        new SimpleContainerDownloader(conf, caClient);
    Path file = downloader.getContainerDataFromReplicas(containerId,
        sourceDatanodes, tempFolder.resolve("tmp"), NO_COMPRESSION);
    downloader.close();
    assertNull(file);
    assertThat(logCapture.getOutput())
        .contains("java.security.cert.CertificateExpiredException");
  }

  private void assertDownloadContainerWorks(List<Long> containers,
      List<DatanodeDetails> sourceDatanodes) {
    for (Long cId : containers) {
      SimpleContainerDownloader downloader =
          new SimpleContainerDownloader(conf, caClient);
      Path file = downloader.getContainerDataFromReplicas(cId, sourceDatanodes,
          tempFolder.resolve("tmp"), NO_COMPRESSION);
      downloader.close();
      assertNotNull(file);
    }
  }

  private Token<ContainerTokenIdentifier> createContainer(
      XceiverClientSpi client, boolean useToken, long id) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Token<ContainerTokenIdentifier> token = useToken
        ? secretManager.generateToken(ugi.getUserName(), valueOf(id))
        : null;

    ContainerCommandRequestProto request =
        getCreateContainerSecureRequest(id, client.getPipeline(), token);
    ContainerCommandResponseProto response = client.sendCommand(request);
    assertNotNull(response);
    assertSame(response.getResult(), ContainerProtos.Result.SUCCESS);
    return token;
  }

  private long createAndCloseContainer(
      XceiverClientSpi client, boolean useToken) throws IOException {
    long id = getTestContainerID();
    Token<ContainerTokenIdentifier> token = createContainer(client, useToken, id);

    ContainerCommandRequestProto request =
        getCloseContainer(client.getPipeline(), id, token);
    ContainerCommandResponseProto response = client.sendCommand(request);
    assertNotNull(response);
    assertSame(response.getResult(), ContainerProtos.Result.SUCCESS);
    return id;
  }

  private void letCertExpire() throws Exception {
    Date expiry = caClient.getCertificate().getNotAfter();
    waitFor(() -> expiry.before(new Date()), 100, CERT_LIFETIME * 1000);
  }

  private void letCACertExpire() throws Exception {
    Date expiry = caClient.getCACertificate().getNotAfter();
    waitFor(() -> expiry.before(new Date()), 100, ROOT_CERT_LIFE_TIME * 1000);
  }

  private ClientTrustManager aClientTrustManager() throws IOException {
    X509Certificate firstCert = caClient.getCACertificate();
    return new ClientTrustManager(
        () -> singletonList(caClient.getCACertificate()),
        () -> singletonList(firstCert));
  }

  private DatanodeDetails aDatanode() {
    return MockDatanodeDetails.createDatanodeDetails(
        DatanodeID.randomID(), "localhost", "0.0.0.0",
        "/default-rack");
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.token.ContainerTokenIdentifier;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.SecretKeyTestClient;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.container.replication.ContainerDownloader;
import org.apache.hadoop.ozone.container.replication.SimpleContainerDownloader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.CertificateExpiredException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_LEN;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_ACK_TIMEOUT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_CHECK_INTERNAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DEFAULT_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getTestContainerID;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getMockContext;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests ozone containers via secure grpc/netty.
 */
@Timeout(300)
class TestOzoneContainerWithTLS {

  @TempDir
  private Path tempFolder;

  private OzoneConfiguration conf;
  private ContainerTokenSecretManager secretManager;
  private CertificateClientTestImpl caClient;
  private SecretKeyClient secretKeyClient;
  private final Duration certLifetime = Duration.ofSeconds(15);

  @BeforeEach
  void setup() throws Exception {
    conf = new OzoneConfiguration();

    conf.set(OZONE_METADATA_DIRS,
        tempFolder.resolve("meta").toString());
    conf.set(HDDS_DATANODE_DIR_KEY,
        tempFolder.resolve("data").toString());
    conf.set(HDDS_DATANODE_CONTAINER_DB_DIR,
        tempFolder.resolve("containers").toString());

    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(HddsConfigKeys.HDDS_GRPC_TLS_ENABLED, true);

    conf.setBoolean(HddsConfigKeys.HDDS_GRPC_TLS_TEST_CERT, true);
    conf.setBoolean(HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED, false);
    conf.setInt(HDDS_KEY_LEN, 1024);

    conf.set(HDDS_X509_DEFAULT_DURATION, certLifetime.toString());
    conf.set(HDDS_X509_RENEW_GRACE_DURATION, "PT2S");
    conf.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "PT1S"); // 1s
    conf.set(HDDS_X509_CA_ROTATION_ACK_TIMEOUT, "PT1S"); // 1s

    long expiryTime = conf.getTimeDuration(
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME, "1s",
        TimeUnit.MILLISECONDS);

    caClient = new CertificateClientTestImpl(conf);
    secretKeyClient = new SecretKeyTestClient();
    secretManager = new ContainerTokenSecretManager(expiryTime,
        secretKeyClient);
  }

  @Test
  void testCertificateLifetime() {
    Date afterExpiry = Date.from(LocalDateTime.now()
        .plus(certLifetime)
        .atZone(ZoneId.systemDefault())
        .toInstant());

    assertThrows(CertificateExpiredException.class,
        () -> caClient.getCertificate().checkValidity(afterExpiry));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void createContainer(boolean containerTokenEnabled) throws Exception {
    conf.setBoolean(HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED,
        containerTokenEnabled);

    final long containerId = getTestContainerID();
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    OzoneContainer container = null;
    try {
      Pipeline pipeline = MockPipeline.createSingleNodePipeline();
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          pipeline.getFirstNode().getPort(DatanodeDetails.Port.Name.STANDALONE)
              .getValue());
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, false);

      container = new OzoneContainer(dn, conf, getMockContext(dn, conf),
          caClient, secretKeyClient);
      //Set scmId and manually start ozone container.
      container.start(UUID.randomUUID().toString());

      try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf,
          singletonList(caClient.getCACertificate()))) {

        if (containerTokenEnabled) {
          client.connect();
          createSecureContainer(client, containerId,
              secretManager.generateToken(
                  UserGroupInformation.getCurrentUser().getUserName(),
                  ContainerID.valueOf(containerId)));
        } else {
          client.connect();
          createContainer(client, containerId);
        }
      }
    } finally {
      if (container != null) {
        container.stop();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void downloadContainer(boolean tokenEnabled) throws Exception {
    DatanodeDetails dn = MockDatanodeDetails.createDatanodeDetails(
        UUID.randomUUID().toString(), "localhost", "0.0.0.0",
        "/default-rack");
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
        pipeline.getFirstNode().getPort(DatanodeDetails.Port.Name.STANDALONE)
            .getValue());
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, false);

    OzoneContainer container = null;
    try {
      container = new OzoneContainer(dn, conf,
          getMockContext(dn, conf), caClient, secretKeyClient);

      // Set scmId and manually start ozone container.
      container.start(UUID.randomUUID().toString());

      // Create containers
      try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf,
          singletonList(caClient.getCACertificate()))) {
        client.connect();

        final long containerId = getTestContainerID();
        createAndCloseContainer(tokenEnabled, containerId, client);

        // Wait certificate to expire
        GenericTestUtils.waitFor(
            () -> caClient.getCertificate().getNotAfter().before(new Date()),
            100, (int) certLifetime.toMillis());

        // old client still function well after certificate expired
        createAndCloseContainer(tokenEnabled, getTestContainerID(), client);

        // Download newly created container will fail because of cert expired
        GenericTestUtils.LogCapturer logCapture = GenericTestUtils.LogCapturer
            .captureLogs(SimpleContainerDownloader.LOG);
        assertNull(downloadContainer(containerId, dn));
        assertThat(logCapture.getOutput(),
            containsString(CertificateExpiredException.class.getName()));

        // Renew the certificate
        caClient.renewKey();

        // old client still function well after certificate renewed
        createAndCloseContainer(tokenEnabled, getTestContainerID(), client);

        // Wait keyManager and trustManager to reload
        Thread.sleep(2000); // TODO replace

        // old client still function well after certificate reload
        createAndCloseContainer(tokenEnabled, getTestContainerID(), client);

        // Download container should succeed after key and cert renewed
        assertNotNull(downloadContainer(containerId, dn));
      }
    } finally {
      if (container != null) {
        container.stop();
      }
      // TODO delete leftover hadoop-ozone/integration-test/container.db
    }
  }

  private Path downloadContainer(long containerId, DatanodeDetails source)
      throws IOException {
    try (ContainerDownloader downloader = new SimpleContainerDownloader(
        conf, caClient)) {
      return downloader.getContainerDataFromReplicas(containerId,
          singletonList(source), tempFolder.resolve("tmp"), NO_COMPRESSION);
    }
  }

  private void createAndCloseContainer(boolean containerTokenEnabled,
      long containerId, XceiverClientGrpc client) throws Exception {
    if (containerTokenEnabled) {
      Token<ContainerTokenIdentifier> token = secretManager.generateToken(
          UserGroupInformation.getCurrentUser().getUserName(),
          ContainerID.valueOf(containerId));
      createSecureContainer(client, containerId, token);
      closeSecureContainer(client, containerId, token);
    } else {
      createContainer(client, containerId);
      closeContainer(client, containerId);
    }
  }

  public static void createContainer(XceiverClientSpi client,
      long containerID) throws Exception {
    ContainerCommandRequestProto request = ContainerTestHelper
        .getCreateContainerRequest(containerID, client.getPipeline());
    ContainerCommandResponseProto response = client.sendCommand(request);
    assertNotNull(response);
    assertSame(ContainerProtos.Result.SUCCESS, response.getResult());
  }

  public static void createSecureContainer(XceiverClientSpi client,
      long containerID, Token<ContainerTokenIdentifier> token)
      throws Exception {
    ContainerCommandRequestProto request =
        ContainerTestHelper.getCreateContainerSecureRequest(
            containerID, client.getPipeline(), token);
    ContainerCommandResponseProto response =
        client.sendCommand(request);
    assertNotNull(response);
    assertSame(ContainerProtos.Result.SUCCESS, response.getResult());
  }

  public static void closeContainer(XceiverClientSpi client,
      long containerID) throws Exception {
    ContainerCommandRequestProto request = ContainerTestHelper
        .getCloseContainer(client.getPipeline(), containerID);
    ContainerCommandResponseProto response = client.sendCommand(request);
    assertNotNull(response);
    assertSame(ContainerProtos.Result.SUCCESS, response.getResult());
  }

  public static void closeSecureContainer(XceiverClientSpi client,
      long containerID, Token<ContainerTokenIdentifier> token)
      throws Exception {
    ContainerCommandRequestProto request =
        ContainerTestHelper.getCloseContainer(client.getPipeline(),
            containerID, token);
    ContainerCommandResponseProto response =
        client.sendCommand(request);
    assertNotNull(response);
    assertSame(ContainerProtos.Result.SUCCESS, response.getResult());
  }
}

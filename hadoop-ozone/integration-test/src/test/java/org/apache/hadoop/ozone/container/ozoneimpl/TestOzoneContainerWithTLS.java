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

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.security.token.ContainerTokenIdentifier;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.replication.SimpleContainerDownloader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.security.cert.CertificateExpiredException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_LEN;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_SSL_KEYSTORE_RELOAD_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_SSL_TRUSTSTORE_RELOAD_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DEFAULT_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;

/**
 * Tests ozone containers via secure grpc/netty.
 */
@RunWith(Parameterized.class)
public class TestOzoneContainerWithTLS {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestOzoneContainerWithTLS.class);
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = Timeout.seconds(300);

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private ContainerTokenSecretManager secretManager;
  private CertificateClientTestImpl caClient;
  private boolean containerTokenEnabled;
  private int certLifetime = 15 * 1000; // 15s

  public TestOzoneContainerWithTLS(boolean enableToken) {
    this.containerTokenEnabled = enableToken;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> enableBlockToken() {
    return Arrays.asList(new Object[][] {
        {false},
        {true}
    });
  }

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    String ozoneMetaPath =
        GenericTestUtils.getTempPath("ozoneMeta");
    File ozoneMetaFile = new File(ozoneMetaPath);
    conf.set(OZONE_METADATA_DIRS, ozoneMetaPath);

    FileUtil.fullyDelete(ozoneMetaFile);
    String keyDirName = conf.get(HDDS_KEY_DIR_NAME,
        HDDS_KEY_DIR_NAME_DEFAULT);

    File ozoneKeyDir = new File(ozoneMetaFile, keyDirName);
    ozoneKeyDir.mkdirs();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(HddsConfigKeys.HDDS_GRPC_TLS_ENABLED, true);

    conf.setBoolean(HddsConfigKeys.HDDS_GRPC_TLS_TEST_CERT, true);
    conf.setInt(HDDS_KEY_LEN, 1024);

    // certificate lives for 10s
    conf.set(HDDS_X509_DEFAULT_DURATION,
        Duration.ofMillis(certLifetime).toString());
    conf.set(HDDS_X509_RENEW_GRACE_DURATION, "PT2S");
    conf.set(HDDS_SECURITY_SSL_KEYSTORE_RELOAD_INTERVAL, "1s");
    conf.set(HDDS_SECURITY_SSL_TRUSTSTORE_RELOAD_INTERVAL, "1s");

    long expiryTime = conf.getTimeDuration(
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME, "1s",
        TimeUnit.MILLISECONDS);

    caClient = new CertificateClientTestImpl(conf);
    secretManager = new ContainerTokenSecretManager(new SecurityConfig(conf),
        expiryTime);
  }

  @Test(expected = CertificateExpiredException.class)
  public void testCertificateLifetime() throws Exception {
    // Sleep to wait for certificate expire
    LocalDateTime now = LocalDateTime.now();
    now = now.plusSeconds(certLifetime / 1000);
    caClient.getCertificate().checkValidity(Date.from(
        now.atZone(ZoneId.systemDefault()).toInstant()));
  }

  @Test
  public void testCreateOzoneContainer() throws Exception {
    LOG.info("testCreateOzoneContainer with TLS and containerToken enabled: {}",
        containerTokenEnabled);
    conf.setBoolean(HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED,
        containerTokenEnabled);

    long containerId = ContainerTestHelper.getTestContainerID();
    OzoneContainer container = null;
    System.out.println(System.getProperties().getProperty("java.library.path"));
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    try {
      Pipeline pipeline = MockPipeline.createSingleNodePipeline();
      conf.set(HDDS_DATANODE_DIR_KEY, tempFolder.getRoot().getPath());
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          pipeline.getFirstNode().getPort(DatanodeDetails.Port.Name.STANDALONE)
              .getValue());
      conf.setBoolean(
          OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, false);

      container = new OzoneContainer(dn, conf, getContext(dn), caClient);
      //Set scmId and manually start ozone container.
      container.start(UUID.randomUUID().toString());

      XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf,
          Collections.singletonList(caClient.getCACertificate()));

      if (containerTokenEnabled) {
        secretManager.start(caClient);
        client.connect();
        createSecureContainer(client, containerId,
            secretManager.generateToken(
                UserGroupInformation.getCurrentUser().getUserName(),
                ContainerID.valueOf(containerId)));
      } else {
        client.connect();
        createContainer(client, containerId);
      }
    } finally {
      if (container != null) {
        container.stop();
      }
    }
  }

  @Test
  public void testContainerDownload() throws Exception {
    DatanodeDetails dn = MockDatanodeDetails.createDatanodeDetails(
        UUID.randomUUID().toString(), "localhost", "0.0.0.0",
        "/default-rack");
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    conf.set(HDDS_DATANODE_DIR_KEY, tempFolder.newFolder().getPath());
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
        pipeline.getFirstNode().getPort(DatanodeDetails.Port.Name.STANDALONE)
            .getValue());
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, false);

    OzoneContainer container = null;
    try {
      container = new OzoneContainer(dn, conf, getContext(dn), caClient);

      // Set scmId and manually start ozone container.
      container.start(UUID.randomUUID().toString());

      if (containerTokenEnabled) {
        secretManager.start(caClient);
      }

      // Create containers
      long containerId = ContainerTestHelper.getTestContainerID();
      List<Long> containerIdList = new ArrayList<>();
      XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf,
          Collections.singletonList(caClient.getCACertificate()));
      client.connect();
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
      containerIdList.add(containerId++);

      // Wait certificate to expire
      GenericTestUtils.waitFor(() ->
              caClient.getCertificate().getNotAfter().before(new Date()),
          100, certLifetime);

      List<DatanodeDetails> sourceDatanodes = new ArrayList<>();
      sourceDatanodes.add(dn);
      if (containerTokenEnabled) {
        // old client still function well after certificate expired
        Token<ContainerTokenIdentifier> token = secretManager.generateToken(
            UserGroupInformation.getCurrentUser().getUserName(),
            ContainerID.valueOf(containerId));
        createSecureContainer(client, containerId, token);
        closeSecureContainer(client, containerId++, token);
      } else {
        createContainer(client, containerId);
        closeContainer(client, containerId++);
      }

      // Download newly created container will fail because of cert expired
      GenericTestUtils.LogCapturer logCapture = GenericTestUtils.LogCapturer
          .captureLogs(SimpleContainerDownloader.LOG);
      SimpleContainerDownloader downloader =
          new SimpleContainerDownloader(conf, caClient);
      Path file = downloader.getContainerDataFromReplicas(
          containerId, sourceDatanodes, null, NO_COMPRESSION);
      downloader.close();
      Assert.assertNull(file);
      Assert.assertTrue(logCapture.getOutput().contains(
          "java.security.cert.CertificateExpiredException"));

      // Renew the certificate
      caClient.renewKey();

      // old client still function well after certificate renewed
      if (containerTokenEnabled) {
        Token<ContainerTokenIdentifier> token = secretManager.generateToken(
            UserGroupInformation.getCurrentUser().getUserName(),
            ContainerID.valueOf(containerId));
        createSecureContainer(client, containerId, token);
        closeSecureContainer(client, containerId++, token);
      }

      // Wait keyManager and trustManager to reload
      Thread.sleep(2000);

      // old client still function well after certificate reload
      if (containerTokenEnabled) {
        Token<ContainerTokenIdentifier> token = secretManager.generateToken(
            UserGroupInformation.getCurrentUser().getUserName(),
            ContainerID.valueOf(containerId));
        createSecureContainer(client, containerId, token);
        closeSecureContainer(client, containerId++, token);
      } else {
        createContainer(client, containerId);
        closeContainer(client, containerId++);
      }

      // Download container should succeed after key and cert renewed
      for (Long cId : containerIdList) {
        downloader = new SimpleContainerDownloader(conf, caClient);
        try {
          file = downloader.getContainerDataFromReplicas(cId, sourceDatanodes,
                  null, NO_COMPRESSION);
          downloader.close();
          Assert.assertNotNull(file);
        } finally {
          if (downloader != null) {
            downloader.close();
          }
          client.close();
        }
      }
    } finally {
      if (container != null) {
        container.stop();
      }
    }
  }

  public static void createContainer(XceiverClientSpi client,
      long containerID) throws Exception {
    ContainerCommandRequestProto request = ContainerTestHelper
        .getCreateContainerRequest(containerID, client.getPipeline());
    ContainerCommandResponseProto response = client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertTrue(response.getResult() == ContainerProtos.Result.SUCCESS);
  }

  public static void createSecureContainer(XceiverClientSpi client,
      long containerID, Token<ContainerTokenIdentifier> token)
      throws Exception {
    ContainerCommandRequestProto request =
        ContainerTestHelper.getCreateContainerSecureRequest(
            containerID, client.getPipeline(), token);
    ContainerCommandResponseProto response =
        client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertTrue(response.getResult() == ContainerProtos.Result.SUCCESS);
  }

  public static void closeContainer(XceiverClientSpi client,
      long containerID) throws Exception {
    ContainerCommandRequestProto request = ContainerTestHelper
        .getCloseContainer(client.getPipeline(), containerID);
    ContainerCommandResponseProto response = client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertTrue(response.getResult() == ContainerProtos.Result.SUCCESS);
  }

  public static void closeSecureContainer(XceiverClientSpi client,
      long containerID, Token<ContainerTokenIdentifier> token)
      throws Exception {
    ContainerCommandRequestProto request =
        ContainerTestHelper.getCloseContainer(client.getPipeline(),
            containerID, token);
    ContainerCommandResponseProto response =
        client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertTrue(response.getResult() == ContainerProtos.Result.SUCCESS);
  }

  private StateContext getContext(DatanodeDetails datanodeDetails) {
    DatanodeStateMachine stateMachine = Mockito.mock(
        DatanodeStateMachine.class);
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    return context;
  }
}

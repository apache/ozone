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

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getCreateContainerSecureRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getTestContainerID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.token.ContainerTokenIdentifier;
import org.apache.hadoop.hdds.security.token.ContainerTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.SecretKeyTestClient;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests ozone containers via secure grpc/netty.
 */
class TestSecureOzoneContainer {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestSecureOzoneContainer.class);

  @TempDir
  private Path tempFolder;
  @TempDir
  private Path ozoneMetaPath;

  private OzoneConfiguration conf;
  private VolumeChoosingPolicy volumeChoosingPolicy;
  private CertificateClientTestImpl caClient;
  private SecretKeyClient secretKeyClient;
  private ContainerTokenSecretManager secretManager;

  static Collection<Arguments> blockTokenOptions() {
    return Arrays.asList(
        Arguments.arguments(true, true, false),
        Arguments.arguments(true, true, true),
        Arguments.arguments(true, false, false),
        Arguments.arguments(false, true, false),
        Arguments.arguments(false, false, false)
    );
  }

  @BeforeAll
  static void init() {
    DefaultMetricsSystem.setMiniClusterMode(true);
    ExitUtils.disableSystemExit();
  }

  @BeforeEach
  void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, ozoneMetaPath.toString());
    caClient = new CertificateClientTestImpl(conf);
    secretKeyClient = new SecretKeyTestClient();
    secretManager = new ContainerTokenSecretManager(
        TimeUnit.DAYS.toMillis(1), secretKeyClient);
    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);
  }

  @ParameterizedTest
  @MethodSource("blockTokenOptions")
  void testCreateOzoneContainer(boolean requireToken, boolean hasToken,
      boolean tokenExpired) throws Exception {
    final String testCase = testCase(requireToken, hasToken, tokenExpired);
    LOG.info("Test case: {}", testCase);

    conf.setBoolean(HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED, requireToken);
    conf.setBoolean(HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED, requireToken);

    ContainerID containerID = ContainerID.valueOf(getTestContainerID());
    OzoneContainer container = null;
    try {
      Pipeline pipeline = MockPipeline.createSingleNodePipeline();
      conf.set(HDDS_DATANODE_DIR_KEY, tempFolder.toString());
      conf.setInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT, pipeline
          .getFirstNode().getStandalonePort().getValue());
      conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT, false);

      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      container = new OzoneContainer(null, dn, conf, ContainerTestUtils
          .getMockContext(dn, conf), caClient, secretKeyClient, volumeChoosingPolicy);
      MutableVolumeSet volumeSet = container.getVolumeSet();
      StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())
          .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempFolder.toFile()));
      ContainerTestUtils.initializeDatanodeLayout(conf, dn);
      //Set scmId and manually start ozone container.
      container.start(UUID.randomUUID().toString());

      String user = "user1";
      UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
          user,  new String[] {"usergroup"});

      ugi.doAs((PrivilegedAction<Void>) () -> {
        try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf)) {
          client.connect();

          Token<?> token = null;
          if (hasToken) {
            Instant expiryDate = tokenExpired
                ? Instant.now().minusSeconds(3600)
                : Instant.now().plusSeconds(3600);
            ContainerTokenIdentifier tokenIdentifier =
                new ContainerTokenIdentifier(user, containerID,
                    secretKeyClient.getCurrentSecretKey().getId(),
                    expiryDate);
            token = secretManager.generateToken(tokenIdentifier);
          }

          ContainerCommandRequestProto request =
              getCreateContainerSecureRequest(containerID.getId(),
                  client.getPipeline(), token);
          ContainerCommandResponseProto response = client.sendCommand(request);
          assertNotNull(response);
          ContainerProtos.Result expectedResult =
              !requireToken || (hasToken && !tokenExpired)
                  ? ContainerProtos.Result.SUCCESS
                  : ContainerProtos.Result.BLOCK_TOKEN_VERIFICATION_FAILED;
          assertEquals(expectedResult, response.getResult(), testCase);
        } catch (SCMSecurityException e) {
          assertTrue(requireToken && hasToken && tokenExpired, testCase);
        } catch (IOException e) {
          assertTrue(requireToken && !hasToken, testCase);
        } catch (Exception e) {
          fail(e);
        }
        return null;
      });
    } finally {
      if (container != null) {
        container.stop();
      }
    }
  }

  private String testCase(boolean requireToken, boolean hasToken,
      boolean tokenExpired) {
    if (!requireToken) {
      return "unsecure";
    }
    if (!hasToken) {
      return "unauthorized";
    }
    if (tokenExpired) {
      return "token expired";
    }
    return "valid token";
  }
}

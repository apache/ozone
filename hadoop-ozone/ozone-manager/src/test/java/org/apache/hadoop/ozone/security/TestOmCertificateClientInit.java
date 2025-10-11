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

package org.apache.hadoop.ozone.security;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.FAILURE;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.GETCERT;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for {@link OMCertificateClient}.
 */
public class TestOmCertificateClientInit {

  private KeyPair keyPair;
  private OMCertificateClient omCertificateClient;
  private SecurityConfig securityConfig;
  private KeyStorage omKeyStorage;
  private X509Certificate x509Certificate;
  private static final String OM_COMPONENT = OMCertificateClient.COMPONENT_NAME;

  private static Stream<Arguments> parameters() {
    return Stream.of(
        arguments(false, false, false, GETCERT),
        arguments(false, false, true, FAILURE),
        arguments(false, true, false, FAILURE),
        arguments(true, false, false, GETCERT),
        arguments(false, true, true, FAILURE),
        arguments(true, true, false, GETCERT),
        arguments(true, false, true, SUCCESS),
        arguments(true, true, true, SUCCESS)
    );
  }

  @BeforeEach
  public void setUp(@TempDir Path metaDirPath) throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(HDDS_METADATA_DIR_NAME, metaDirPath.toString());
    securityConfig = new SecurityConfig(config);
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(securityConfig);
    keyPair = keyGenerator.generateKey();
    x509Certificate = getX509Certificate();
    String certSerialId = x509Certificate.getSerialNumber().toString();
    OMStorage storage = mock(OMStorage.class);
    when(storage.getOmCertSerialId()).thenReturn(certSerialId);
    when(storage.getClusterID()).thenReturn("test");
    when(storage.getOmId()).thenReturn(UUID.randomUUID().toString());
    HddsProtos.OzoneManagerDetailsProto omInfo =
        OzoneManager.getOmDetailsProto(config, storage.getOmId());
    omCertificateClient =
        new OMCertificateClient(
            securityConfig, null, storage, omInfo, "", null, null, null);
    omKeyStorage = new KeyStorage(securityConfig, OM_COMPONENT);

    Files.createDirectories(securityConfig.getKeyLocation(OM_COMPONENT));
  }

  @AfterEach
  public void tearDown() throws IOException {
    omCertificateClient.close();
    omCertificateClient = null;
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void testInitOzoneManager(boolean pvtKeyPresent, boolean pubKeyPresent,
      boolean certPresent, InitResponse expectedResult) throws Exception {
    if (pvtKeyPresent) {
      omKeyStorage.storePrivateKey(keyPair.getPrivate());
    } else {
      FileUtils.deleteQuietly(Paths.get(
          securityConfig.getKeyLocation(OM_COMPONENT).toString(),
          securityConfig.getPrivateKeyFileName()).toFile());
    }

    if (pubKeyPresent) {
      if (omCertificateClient.getPublicKey() == null) {
        omKeyStorage.storePublicKey(keyPair.getPublic());
      }
    } else {
      FileUtils.deleteQuietly(Paths.get(
          securityConfig.getKeyLocation(OM_COMPONENT).toString(),
          securityConfig.getPublicKeyFileName()).toFile());
    }

    if (certPresent) {
      CertificateCodec codec = new CertificateCodec(securityConfig,
          OM_COMPONENT);
      codec.writeCertificate(x509Certificate);
    } else {
      FileUtils.deleteQuietly(Paths.get(
          securityConfig.getKeyLocation(OM_COMPONENT).toString(),
          securityConfig.getCertificateFileName()).toFile());
    }
    InitResponse response = omCertificateClient.init();

    if (pvtKeyPresent && pubKeyPresent && !certPresent) {
      assertEquals(GETCERT, response);
    } else {
      assertEquals(expectedResult, response);
    }

    if (!response.equals(FAILURE)) {
      assertTrue(OzoneSecurityUtil.checkIfFileExist(
          securityConfig.getKeyLocation(OM_COMPONENT),
          securityConfig.getPrivateKeyFileName()));
      assertTrue(OzoneSecurityUtil.checkIfFileExist(
          securityConfig.getKeyLocation(OM_COMPONENT),
          securityConfig.getPublicKeyFileName()));
    }
  }

  private X509Certificate getX509Certificate() throws Exception {
    return KeyStoreTestUtil.generateCertificate(
        "CN=Test", keyPair, 365, securityConfig.getSignatureAlgo());
  }
}

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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.apache.hadoop.ozone.security.OMCertificateClient;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test secure Ozone Manager operation in distributed handler scenario.
 */
class TestSecureOzoneManager {

  private static final String COMPONENT = "om";
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omId;
  @TempDir
  private Path metaDir;
  private HddsProtos.OzoneManagerDetailsProto omInfo;

  @BeforeEach
  void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.toString());
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 2);
    conf.set(OZONE_SCM_NAMES, "localhost");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.toString());
    OzoneManager.setTestSecureOmFlag(true);
    omInfo = OzoneManager.getOmDetailsProto(conf, omId);
  }

  /**
   * Test failure cases for secure OM initialization.
   */
  @Test
  void testSecureOmInitFailures() throws Exception {
    PrivateKey privateKey;
    PublicKey publicKey;
    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId(clusterId);
    omStorage.setOmId(omId);

    // Case 1: When keypair as well as certificate is missing. Initial keypair
    // boot-up. Get certificate will fail when SCM is not running.
    SecurityConfig securityConfig = new SecurityConfig(conf);
    OMCertificateClient client =
        new OMCertificateClient(
            securityConfig, null, omStorage, omInfo, "", scmId, null, null);
    assertEquals(CertificateClient.InitResponse.GETCERT, client.init());
    privateKey = client.getPrivateKey();
    publicKey = client.getPublicKey();
    assertNotNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNull(client.getCertificate());
    client.close();

    // Case 2: If key pair already exist than response should be GETCERT.
    client =
        new OMCertificateClient(
            securityConfig, null, omStorage, omInfo, "", scmId, null, null);
    assertEquals(CertificateClient.InitResponse.GETCERT, client.init());
    assertNotNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNull(client.getCertificate());
    client.close();

    // Case 3: When public key as well as certificate is missing.
    client =
        new OMCertificateClient(
            securityConfig, null, omStorage, omInfo, "", null, null, null);
    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation(COMPONENT)
        .toString(), securityConfig.getPublicKeyFileName()).toFile());
    assertEquals(CertificateClient.InitResponse.GETCERT, client.init());
    assertNotNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNull(client.getCertificate());
    client.close();

    // Case 4: When private key and certificate is missing.
    client =
        new OMCertificateClient(
            securityConfig, null, omStorage, omInfo, "", null, null, null);
    KeyStorage keyStorage = new KeyStorage(securityConfig, COMPONENT);
    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation(COMPONENT)
        .toString(), securityConfig.getPrivateKeyFileName()).toFile());
    assertEquals(CertificateClient.InitResponse.FAILURE, client.init());
    assertNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNull(client.getCertificate());
    client.close();

    // Case 5: When only certificate is present.
    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation(COMPONENT)
        .toString(), securityConfig.getPublicKeyFileName()).toFile());
    CertificateCodec certCodec =
        new CertificateCodec(securityConfig, COMPONENT);
    X509Certificate x509Certificate = KeyStoreTestUtil.generateCertificate(
        "CN=Test", new KeyPair(publicKey, privateKey), 365,
        securityConfig.getSignatureAlgo());
    certCodec.writeCertificate(x509Certificate);
    omStorage.setOmCertSerialId(x509Certificate.getSerialNumber().toString());
    client =
        new OMCertificateClient(
            securityConfig, null, omStorage, omInfo, "", scmId, null, null);
    assertEquals(CertificateClient.InitResponse.FAILURE, client.init());
    assertNull(client.getPrivateKey());
    assertNull(client.getPublicKey());
    assertNotNull(client.getCertificate());
    client.close();

    // Case 6: When private key and certificate is present.
    client =
        new OMCertificateClient(
            securityConfig, null, omStorage, omInfo, "", scmId, null, null);
    FileUtils.deleteQuietly(Paths.get(securityConfig.getKeyLocation(COMPONENT)
        .toString(), securityConfig.getPublicKeyFileName()).toFile());
    keyStorage.storePrivateKey(privateKey);
    assertEquals(CertificateClient.InitResponse.SUCCESS, client.init());
    assertNotNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNotNull(client.getCertificate());
    client.close();

    // Case 7 When keypair and certificate is present.
    client =
        new OMCertificateClient(
            securityConfig, null, omStorage, omInfo, "", scmId, null, null);
    assertEquals(CertificateClient.InitResponse.SUCCESS, client.init());
    assertNotNull(client.getPrivateKey());
    assertNotNull(client.getPublicKey());
    assertNotNull(client.getCertificate());
    client.close();
  }

  /**
   * Test om bind socket address.
   */
  @Test
  void testSecureOmInitFailure() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration(conf);
    OMStorage omStorage = new OMStorage(config);
    omStorage.setClusterId(clusterId);
    omStorage.setOmId(omId);
    config.set(OZONE_OM_ADDRESS_KEY, "om-unknown");
    RuntimeException rte = assertThrows(RuntimeException.class,
        () -> OzoneManager.initializeSecurity(config, omStorage, scmId));
    assertThat(rte)
        .hasMessageStartingWith("Can't get SCM signed certificate. omRpcAdd: om-unknown")
        .hasMessageEndingWith(":9862");
  }

}

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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_EXPIRY_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_ACK_TIMEOUT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_CHECK_INTERNAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DEFAULT_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.ROLLBACK_ERROR;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClient;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_SUB_CA;
import static org.apache.hadoop.ozone.om.OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_EXPIRED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.USER_MISMATCH;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.apache.ozone.test.GenericTestUtils.PortAllocator.getFreePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.INFO;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.client.ScmTopologyClient;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultApprover;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.hdds.security.x509.certificate.client.DefaultCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc_.Client;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmBlockLocationTestingClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.security.OMCertificateClient;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.SecretKeyTestClient;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ozone.test.tag.Unhealthy;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to for security enabled Ozone cluster.
 */
final class TestSecureOzoneCluster {

  private static final String COMPONENT = "om";
  private static final String OM_CERT_SERIAL_ID = "9879877970576";
  private static final Logger LOG = LoggerFactory
      .getLogger(TestSecureOzoneCluster.class);

  @TempDir
  private File tempDir;

  private MiniKdc miniKdc;
  private OzoneConfiguration conf;
  private File workDir;
  private File scmKeytab;
  private File spnegoKeytab;
  private File omKeyTab;
  private File testUserKeytab;
  private String testUserPrincipal;
  private StorageContainerManager scm;
  private ScmBlockLocationProtocol scmBlockClient;
  private OzoneManager om;
  private HddsProtos.OzoneManagerDetailsProto omInfo;
  private String host;
  private String clusterId;
  private String scmId;
  private String omId;
  private OzoneManagerProtocolClientSideTranslatorPB omClient;
  private KeyPair keyPair;
  private Path omMetaDirPath;
  private int certGraceTime = 10 * 1000; // 10s
  private int delegationTokenMaxTime = 9 * 1000; // 9s

  @BeforeEach
  void init() {
    try {
      conf = new OzoneConfiguration();
      conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");

      conf.setInt(OZONE_SCM_CLIENT_PORT_KEY, getFreePort());
      conf.setInt(OZONE_SCM_DATANODE_PORT_KEY, getFreePort());
      conf.setInt(OZONE_SCM_BLOCK_CLIENT_PORT_KEY, getFreePort());
      conf.setInt(OZONE_SCM_SECURITY_SERVICE_PORT_KEY, getFreePort());
      conf.setInt(OZONE_SCM_RATIS_PORT_KEY, getFreePort());
      conf.setInt(OZONE_SCM_GRPC_PORT_KEY, getFreePort());
      conf.set(OZONE_OM_ADDRESS_KEY,
          InetAddress.getLocalHost().getCanonicalHostName() + ":" + getFreePort());

      DefaultMetricsSystem.setMiniClusterMode(true);
      ExitUtils.disableSystemExit();
      final String path = tempDir.getAbsolutePath();
      omMetaDirPath = Paths.get(path, "om-meta");
      conf.set(OZONE_METADATA_DIRS, omMetaDirPath.toString());
      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
      conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());
      conf.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL,
          Duration.ofMillis(certGraceTime - 1000).toString());
      conf.set(HDDS_X509_RENEW_GRACE_DURATION,
          Duration.ofMillis(certGraceTime).toString());
      conf.setBoolean(HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED, false);
      conf.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL,
          Duration.ofMillis(certGraceTime - 1000).toString());
      conf.set(HDDS_X509_CA_ROTATION_ACK_TIMEOUT,
          Duration.ofMillis(certGraceTime - 1000).toString());
      conf.setLong(OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
          delegationTokenMaxTime);

      workDir = new File(tempDir, "workdir");
      clusterId = UUID.randomUUID().toString();
      scmId = UUID.randomUUID().toString();
      omId = UUID.randomUUID().toString();
      scmBlockClient = new ScmBlockLocationTestingClient(null, null, 0);

      startMiniKdc();
      setSecureConfig();
      createCredentialsInKDC();
      generateKeyPair();
      omInfo = OzoneManager.getOmDetailsProto(conf, omId);
    } catch (Exception e) {
      LOG.error("Failed to initialize TestSecureOzoneCluster", e);
    }
  }

  @AfterEach
  void stop() throws Exception {
    try {
      stopMiniKdc();
      if (scm != null) {
        scm.stop();
        scm.join();
      }
      if (om != null) {
        om.stop();
        om.join();
      }
      IOUtils.closeQuietly(omClient);
    } catch (Exception e) {
      LOG.error("Failed to stop TestSecureOzoneCluster", e);
    }
  }

  private void createCredentialsInKDC() throws Exception {
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    SCMHTTPServerConfig httpServerConfig =
          conf.getObject(SCMHTTPServerConfig.class);
    createPrincipal(scmKeytab, scmConfig.getKerberosPrincipal());
    createPrincipal(spnegoKeytab, httpServerConfig.getKerberosPrincipal());
    createPrincipal(testUserKeytab, testUserPrincipal);
    createPrincipal(omKeyTab,
        conf.get(OZONE_OM_KERBEROS_PRINCIPAL_KEY));
  }

  private void createPrincipal(File keytab, String... principal)
      throws Exception {
    miniKdc.createPrincipal(keytab, principal);
  }

  private void startMiniKdc() throws Exception {
    Properties securityProperties = MiniKdc.createConf();
    miniKdc = new MiniKdc(securityProperties, workDir);
    miniKdc.start();
  }

  private void stopMiniKdc() {
    miniKdc.stop();
  }

  private void setSecureConfig() throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    host = InetAddress.getLocalHost().getCanonicalHostName()
        .toLowerCase();

    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");

    String curUser = UserGroupInformation.getCurrentUser().getUserName();
    conf.set(OZONE_ADMINISTRATORS, curUser);

    String realm = miniKdc.getRealm();
    String hostAndRealm = host + "@" + realm;
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);
    conf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_SCM/" + hostAndRealm);
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "om/" + hostAndRealm);
    conf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_OM/" + hostAndRealm);

    scmKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");
    omKeyTab = new File(workDir, "om.keytab");
    testUserKeytab = new File(workDir, "testuser.keytab");
    testUserPrincipal = "test@" + realm;

    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        scmKeytab.getAbsolutePath());
    conf.set(HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        omKeyTab.getAbsolutePath());
    conf.set(OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE,
        spnegoKeytab.getAbsolutePath());
  }

  @Test
  void testSecureScmStartupSuccess() throws Exception {

    initSCM();
    scm = HddsTestUtils.getScmSimple(conf);
    //Reads the SCM Info from SCM instance
    try {
      scm.start();
      ScmInfo scmInfo = scm.getClientProtocolServer().getScmInfo();
      assertEquals(clusterId, scmInfo.getClusterId());
      assertEquals(scmId, scmInfo.getScmId());
      assertEquals(2, scm.getScmCertificateClient().getTrustChain().size());
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  @Test
  void testSCMSecurityProtocol() throws Exception {

    initSCM();
    scm = HddsTestUtils.getScmSimple(conf);
    //Reads the SCM Info from SCM instance
    try {
      scm.start();

      // Case 1: User with Kerberos credentials should succeed.
      UserGroupInformation ugi =
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(
              testUserPrincipal, testUserKeytab.getCanonicalPath());
      ugi.setAuthenticationMethod(KERBEROS);
      try (SCMSecurityProtocolClientSideTranslatorPB securityClient =
          getScmSecurityClient(conf, ugi)) {
        assertNotNull(securityClient);
        String caCert = securityClient.getCACertificate();
        assertNotNull(caCert);
        // Get some random certificate, used serial id 100 which will be
        // unavailable as our serial id is time stamp. Serial id 1 is root CA,
        // and it is persisted in DB.
        SCMSecurityException securityException = assertThrows(
            SCMSecurityException.class,
            () -> securityClient.getCertificate("100"));
        assertThat(securityException)
            .hasMessageContaining("Certificate not found");
      }

      // Case 2: User without Kerberos credentials should fail.
      ugi = UserGroupInformation.createRemoteUser("test");
      ugi.setAuthenticationMethod(AuthMethod.TOKEN);
      try (SCMSecurityProtocolClientSideTranslatorPB securityClient =
          getScmSecurityClient(conf, ugi)) {

        String cannotAuthMessage = "Client cannot authenticate via:[KERBEROS]";
        IOException ioException = assertThrows(IOException.class,
            securityClient::getCACertificate);
        assertThat(ioException).hasMessageContaining(cannotAuthMessage);
        ioException = assertThrows(IOException.class,
            () -> securityClient.getCertificate("1"));
        assertThat(ioException).hasMessageContaining(cannotAuthMessage);
      }
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  @Test
  void testAdminAccessControlException() throws Exception {
    initSCM();
    scm = HddsTestUtils.getScmSimple(conf);
    //Reads the SCM Info from SCM instance
    try {
      scm.start();

      //case 1: Run admin command with non-admin user.
      UserGroupInformation ugi =
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          testUserPrincipal, testUserKeytab.getCanonicalPath());
      StorageContainerLocationProtocol scmRpcClient =
          HAUtils.getScmContainerClient(conf, ugi);
      IOException ioException = assertThrows(IOException.class,
          scmRpcClient::forceExitSafeMode);
      assertThat(ioException).hasMessageContaining("Access denied");

      // Case 2: User without Kerberos credentials should fail.
      ugi = UserGroupInformation.createRemoteUser("test");
      ugi.setAuthenticationMethod(AuthMethod.TOKEN);
      scmRpcClient =
          HAUtils.getScmContainerClient(conf, ugi);

      String cannotAuthMessage = "Client cannot authenticate via:[KERBEROS]";
      ioException = assertThrows(IOException.class,
          scmRpcClient::forceExitSafeMode);
      assertThat(ioException).hasMessageContaining(cannotAuthMessage);
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  private void initSCM() throws IOException {
    Path scmPath = new File(tempDir, "scm-meta").toPath();
    Files.createDirectories(scmPath);
    conf.set(OZONE_METADATA_DIRS, scmPath.toString());

    // Use scmInit to properly initialize SCM with all required directories
    StorageContainerManager.scmInit(conf, clusterId);
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    scmId = scmStore.getScmId();

    /*
     * As all these processes run inside the same JVM, there are issues around
     * the Hadoop UGI if different processes run with different principals.
     * In this test, the OM has to contact the SCM to download certs. SCM runs
     * as scm/host@REALM, but the OM logs in as om/host@REALM, and then the test
     * fails, and the OM is unable to contact the SCM due to kerberos login
     * issues. To work around that, have the OM run as the same principal as the
     * SCM, and then the test passes.
     *
     */
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY,
        "om/" + host + "@" + miniKdc.getRealm());
    File keyTab = new File(workDir, "om.keytab");
    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, keyTab.getAbsolutePath());
  }

  @Test
  void testSecureScmStartupFailure() throws Exception {
    initSCM();
    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, "");
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");

    IOException ioException = assertThrows(IOException.class,
        () -> HddsTestUtils.getScmSimple(conf));
    assertThat(ioException)
        .hasMessageContaining("Running in secure mode, but config doesn't have a keytab");

    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY,
        "scm/_HOST@EXAMPLE.com");
    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        "/etc/security/keytabs/scm.keytab");

    testCommonKerberosFailures(
        () -> HddsTestUtils.getScmSimple(conf));

  }

  private void testCommonKerberosFailures(Callable<?> test) {
    KerberosAuthException kerberosAuthException = assertThrows(
        KerberosAuthException.class, test::call);
    assertThat(kerberosAuthException)
        .hasMessageContaining("failure to login: for principal:");

    String invalidValue = "OAuth2";
    conf.set(HADOOP_SECURITY_AUTHENTICATION, invalidValue);
    IllegalArgumentException argumentException =
        assertThrows(IllegalArgumentException.class, test::call);
    assertThat(argumentException)
        .hasMessageContaining("Invalid attribute value for " + HADOOP_SECURITY_AUTHENTICATION + " of " + invalidValue);

    conf.set(HADOOP_SECURITY_AUTHENTICATION, "KERBEROS_SSL");
    AuthenticationException authException = assertThrows(
        AuthenticationException.class,
        test::call);
    assertThat(authException)
        .hasMessageContaining("KERBEROS_SSL authentication method not");
  }

  /**
   * Tests the secure om Initialization Failure.
   */
  @Test
  void testSecureOMInitializationFailure() throws Exception {
    initSCM();
    // Create a secure SCM instance as om client will connect to it
    scm = HddsTestUtils.getScmSimple(conf);
    try {
      scm.start();
      setupOm(conf);
      conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY,
          "non-existent-user@EXAMPLE.com");
      testCommonKerberosFailures(() -> OzoneManager.createOm(conf));
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  /**
   * Tests the secure om Initialization Failure due to delegation token and secret key configuration don't meet
   * requirement.
   */
  @Test
  void testSecureOMDelegationTokenSecretManagerInitializationFailure() throws Exception {
    initSCM();
    // Create a secure SCM instance as om client will connect to it
    scm = HddsTestUtils.getScmSimple(conf);
    try {
      scm.start();
      conf.setTimeDuration(HDDS_SECRET_KEY_EXPIRY_DURATION, 7, TimeUnit.DAYS);
      conf.setTimeDuration(OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY, 7, TimeUnit.DAYS);
      IllegalArgumentException exception = assertThrows(
          IllegalArgumentException.class, () -> setupOm(conf));
      assertTrue(exception.getMessage().contains("Secret key expiry duration hdds.secret.key.expiry.duration "  +
          "should be greater than value of (ozone.manager.delegation.token.max-lifetime + " +
          "ozone.manager.delegation.remover.scan.interval + hdds.secret.key.rotate.duration"));
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  /**
   * Tests the secure om Initialization success.
   */
  @Test
  void testSecureOmInitializationSuccess() throws Exception {
    initSCM();
    // Create a secure SCM instance as om client will connect to it
    scm = HddsTestUtils.getScmSimple(conf);
    LogCapturer logs = LogCapturer.captureLogs(OzoneManager.class);
    GenericTestUtils.setLogLevel(OzoneManager.class, INFO);

    try {
      scm.start();
      setupOm(conf);
      om.start();
    } catch (Exception ex) {
      // Expects timeout failure from scmClient in om but om user login via
      // kerberos should succeed.
      assertThat(logs.getOutput()).contains("Ozone Manager login successful");
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  @Test
  void testAccessControlExceptionOnClient() throws Exception {
    initSCM();
    LogCapturer logs = LogCapturer.captureLogs(OzoneManager.class);
    GenericTestUtils.setLogLevel(OzoneManager.class, INFO);
    try {
      // Create a secure SCM instance as om client will connect to it
      scm = HddsTestUtils.getScmSimple(conf);
      scm.start();

      setupOm(conf);
      om.setCertClient(new CertificateClientTestImpl(conf));
      om.setScmTopologyClient(new ScmTopologyClient(scmBlockClient));
      om.start();
    } catch (Exception ex) {
      // Expects timeout failure from scmClient in om but om user login via
      // kerberos should succeed.
      assertThat(logs.getOutput()).contains("Ozone Manager login successful");
    }
    UserGroupInformation ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
            testUserPrincipal, testUserKeytab.getCanonicalPath());
    ugi.setAuthenticationMethod(KERBEROS);
    OzoneManagerProtocolClientSideTranslatorPB secureClient =
        new OzoneManagerProtocolClientSideTranslatorPB(
            OmTransportFactory.create(conf, ugi, null),
            ClientId.randomId().toString());
    secureClient.createVolume(
        new OmVolumeArgs.Builder().setVolume("vol1")
            .setOwnerName("owner1")
            .setAdminName("admin")
            .build());

    ugi = UserGroupInformation.createUserForTesting(
        "testuser1", new String[] {"test"});

    OzoneManagerProtocolClientSideTranslatorPB unsecureClient =
        new OzoneManagerProtocolClientSideTranslatorPB(
            OmTransportFactory.create(conf, ugi, null),
            ClientId.randomId().toString());
    String exMessage = "org.apache.hadoop.security.AccessControlException: " +
        "Client cannot authenticate via:[TOKEN, KERBEROS]";
    logs = LogCapturer.captureLogs(Client.class);
    IOException ioException = assertThrows(IOException.class,
        () -> unsecureClient.listAllVolumes(null, null, 0));
    assertThat(ioException).hasMessageContaining(exMessage);
    assertEquals(1, StringUtils.countMatches(logs.getOutput(), exMessage),
        "There should be no retry on AccessControlException");
  }

  private void generateKeyPair() throws Exception {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(securityConfig);
    keyPair = keyGenerator.generateKey();
    KeyStorage keyStorage = new KeyStorage(securityConfig, COMPONENT);
    keyStorage.storeKeyPair(keyPair);
  }

  /**
   * Tests delegation token renewal.
   */
  @Test
  void testDelegationTokenRenewal() throws Exception {
    GenericTestUtils.setLogLevel(Server.class, INFO);
    LogCapturer omLogs = LogCapturer.captureLogs(OzoneManager.class);

    // Setup SCM
    initSCM();
    scm = HddsTestUtils.getScmSimple(conf);
    try {
      // Start SCM
      scm.start();

      // Setup secure OM for start.
      int tokenMaxLifetime = 1000;
      conf.setLong(DELEGATION_TOKEN_MAX_LIFETIME_KEY, tokenMaxLifetime);
      setupOm(conf);
      OzoneManager.setTestSecureOmFlag(true);
      om.setCertClient(new CertificateClientTestImpl(conf));
      om.setScmTopologyClient(new ScmTopologyClient(scmBlockClient));
      om.start();

      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(conf, ugi, null),
          RandomStringUtils.secure().nextAscii(5));

      // Since client is already connected get a delegation token
      Token<OzoneTokenIdentifier> token = omClient.getDelegationToken(
          new Text("om"));

      // Check if token is of right kind and renewer is running om instance
      assertNotNull(token);
      assertEquals("OzoneToken", token.getKind().toString());
      assertEquals(SecurityUtil.buildTokenService(
          om.getNodeDetails().getRpcAddress()).toString(),
          token.getService().toString());

      // Renew delegation token
      long expiryTime = omClient.renewDelegationToken(token);
      assertThat(expiryTime).isGreaterThan(0);
      omLogs.clearOutput();

      // Test failure of delegation renewal
      // 1. When token maxExpiryTime exceeds
      Thread.sleep(tokenMaxLifetime);
      OMException ex = assertThrows(OMException.class,
          () -> omClient.renewDelegationToken(token));
      assertEquals(TOKEN_EXPIRED, ex.getResult());
      omLogs.clearOutput();

      // 2. When renewer doesn't match (implicitly covers when renewer is
      // null or empty )
      Token<OzoneTokenIdentifier> token2 = omClient.getDelegationToken(
          new Text("randomService"));
      assertNotNull(token2);
      ex = assertThrows(OMException.class,
          () -> omClient.renewDelegationToken(token2));
      assertThat(ex).hasMessageContaining("Delegation token renewal failed");
      assertThat(omLogs.getOutput()).contains(" with non-matching renewer randomService");
      omLogs.clearOutput();

      // 3. Test tampered token
      OzoneTokenIdentifier tokenId = OzoneTokenIdentifier.readProtoBuf(
          token.getIdentifier());
      tokenId.setRenewer(new Text("om"));
      tokenId.setMaxDate(System.currentTimeMillis() * 2);
      Token<OzoneTokenIdentifier> tamperedToken = new Token<>(
          tokenId.getBytes(), token2.getPassword(), token2.getKind(),
          token2.getService());
      ex = assertThrows(OMException.class,
          () -> omClient.renewDelegationToken(tamperedToken));
      assertThat(ex).hasMessageContaining("Delegation token renewal failed");
      assertThat(omLogs.getOutput()).contains("can't be found in cache");
      omLogs.clearOutput();
    } finally {
      if (scm != null) {
        scm.stop();
      }
      IOUtils.closeQuietly(om);
    }
  }

  private void setupOm(OzoneConfiguration config) throws Exception {
    OMStorage omStore = new OMStorage(config);
    omStore.setClusterId(clusterId);
    omStore.setOmCertSerialId(OM_CERT_SERIAL_ID);
    // writes the version file properties
    omStore.initialize();
    OzoneManager.setTestSecureOmFlag(true);
    om = OzoneManager.createOm(config);
  }

  @Test
  @Flaky("HDDS-9349")
  void testGetSetRevokeS3Secret() throws Exception {
    initSCM();
    try {
      scm = HddsTestUtils.getScmSimple(conf);
      scm.start();

      // Setup secure OM for start
      setupOm(conf);
      // Start OM
      om.setCertClient(new CertificateClientTestImpl(conf));
      om.setScmTopologyClient(new ScmTopologyClient(scmBlockClient));
      om.start();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String username = ugi.getUserName();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(conf, ugi, null),
          RandomStringUtils.secure().nextAscii(5));

      // Creates a secret since it does not exist
      S3SecretValue attempt1 = omClient.getS3Secret(username);

      // A second getS3Secret on the same username should throw exception
      try {
        omClient.getS3Secret(username);
      } catch (OMException omEx) {
        assertEquals(OMException.ResultCodes.S3_SECRET_ALREADY_EXISTS,
            omEx.getResult());
      }

      // Revoke the existing secret
      omClient.revokeS3Secret(username);

      // Set secret should fail since the accessId is revoked
      final String secretKeySet = "somesecret1";
      try {
        omClient.setS3Secret(username, secretKeySet);
      } catch (OMException omEx) {
        assertEquals(OMException.ResultCodes.ACCESS_ID_NOT_FOUND,
            omEx.getResult());
      }

      // Get a new secret
      S3SecretValue attempt3 = omClient.getS3Secret(username);

      // secret should differ because it has been revoked previously
      assertNotEquals(attempt3.getAwsSecret(), attempt1.getAwsSecret());

      // accessKey is still the same because it is derived from username
      assertEquals(attempt3.getAwsAccessKey(), attempt1.getAwsAccessKey());

      // Admin can set secret for any user
      S3SecretValue attempt4 = omClient.setS3Secret(username, secretKeySet);
      assertEquals(secretKeySet, attempt4.getAwsSecret());

      // A second getS3Secret on the same username should throw exception
      try {
        omClient.getS3Secret(username);
      } catch (OMException omEx) {
        assertEquals(OMException.ResultCodes.S3_SECRET_ALREADY_EXISTS,
            omEx.getResult());
      }

      // Clean up
      omClient.revokeS3Secret(username);

      // Admin can get and revoke other users' secrets
      // omClient's ugi is current user, which is added as an OM admin
      omClient.getS3Secret("HADOOP/ALICE");
      omClient.revokeS3Secret("HADOOP/ALICE");

      // testUser is not an admin
      final UserGroupInformation ugiNonAdmin =
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(
              testUserPrincipal, testUserKeytab.getCanonicalPath());
      final OzoneManagerProtocolClientSideTranslatorPB omClientNonAdmin =
          new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(conf, ugiNonAdmin, null),
          RandomStringUtils.secure().nextAscii(5));

      OMException omException = assertThrows(OMException.class,
          () -> omClientNonAdmin.getS3Secret("HADOOP/JOHN"));
      assertSame(USER_MISMATCH, omException.getResult());
      omException = assertThrows(OMException.class,
          () -> omClientNonAdmin.revokeS3Secret("HADOOP/DOE"));
      assertSame(USER_MISMATCH, omException.getResult());

    } finally {
      if (scm != null) {
        scm.stop();
      }
      IOUtils.closeQuietly(om);
    }
  }

  /**
   * Tests functionality to init secure OM when it is already initialized.
   */
  @Test
  void testSecureOmReInit() throws Exception {
    LogCapturer omLogs = LogCapturer.captureLogs(OMCertificateClient.class);
    omLogs.clearOutput();

    initSCM();
    try {
      scm = HddsTestUtils.getScmSimple(conf);
      scm.start();
      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, false);
      OMStorage omStore = new OMStorage(conf);
      initializeOmStorage(omStore);
      OzoneManager.setTestSecureOmFlag(true);
      om = OzoneManager.createOm(conf);

      assertNull(om.getCertificateClient());
      String logOutput = omLogs.getOutput();
      assertThat(logOutput)
          .doesNotContain("Init response: GETCERT");
      assertThat(logOutput)
          .doesNotContain("Successfully stored SCM signed certificate");

      if (om.stop()) {
        om.join();
      }

      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
      conf.setBoolean(OZONE_OM_S3_GPRC_SERVER_ENABLED, true);
      conf.set(OZONE_OM_ADDRESS_KEY,
          InetAddress.getLocalHost().getCanonicalHostName() + ":" + getFreePort());

      OzoneManager.omInit(conf);
      om = OzoneManager.createOm(conf);

      assertNotNull(om.getCertificateClient());
      assertNotNull(om.getCertificateClient().getPublicKey());
      assertNotNull(om.getCertificateClient().getPrivateKey());
      assertNotNull(om.getCertificateClient().getCertificate());
      assertThat(omLogs.getOutput())
          .contains("Init response: GETCERT")
          .contains("Successfully stored OM signed certificate");
      X509Certificate certificate = om.getCertificateClient().getCertificate();
      validateCertificate(certificate);

    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  /**
   * Test functionality to get SCM signed certificate for OM.
   */
  @Test
  void testSecureOmInitSuccess() throws Exception {
    LogCapturer omLogs =
        LogCapturer.captureLogs(OMCertificateClient.class);
    omLogs.clearOutput();
    initSCM();
    try {
      scm = HddsTestUtils.getScmSimple(conf);
      scm.start();

      OMStorage omStore = new OMStorage(conf);
      initializeOmStorage(omStore);
      OzoneManager.setTestSecureOmFlag(true);
      om = OzoneManager.createOm(conf);

      assertNotNull(om.getCertificateClient());
      assertNotNull(om.getCertificateClient().getPublicKey());
      assertNotNull(om.getCertificateClient().getPrivateKey());
      assertNotNull(om.getCertificateClient().getCertificate());
      assertEquals(3, om.getCertificateClient().getTrustChain().size());
      assertThat(omLogs.getOutput())
          .contains("Init response: GETCERT")
          .contains("Successfully stored OM signed certificate");
      X509Certificate certificate = om.getCertificateClient().getCertificate();
      validateCertificate(certificate);
      String pemEncodedCACert =
          scm.getSecurityProtocolServer().getCACertificate();
      X509Certificate caCert =
          CertificateCodec.getX509Certificate(pemEncodedCACert);
      X509Certificate caCertStored = om.getCertificateClient()
          .getCertificate(caCert.getSerialNumber().toString());
      assertEquals(caCert, caCertStored);
    } finally {
      if (scm != null) {
        scm.stop();
      }
      if (om != null) {
        om.stop();
      }
      IOUtils.closeQuietly(om);
    }
  }

  /**
   * Test successful certificate rotation.
   */
  @Test
  void testCertificateRotation() throws Exception {
    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId(clusterId);
    omStorage.setOmId(omId);
    OzoneManager.setTestSecureOmFlag(true);

    SecurityConfig securityConfig = new SecurityConfig(conf);

    // save first cert
    final int certificateLifetime = 20; // seconds
    KeyStorage keyStorage = new KeyStorage(securityConfig, "om");
    X509Certificate cert = generateSelfSignedX509Cert(securityConfig,
        keyStorage.readKeyPair(), null, Duration.ofSeconds(certificateLifetime));
    String certId = cert.getSerialNumber().toString();
    omStorage.setOmCertSerialId(certId);
    omStorage.forceInitialize();
    CertificateCodec certCodec = new CertificateCodec(securityConfig, "om");
    certCodec.writeCertificate(cert);
    String caCertFileName = CAType.ROOT.getFileNamePrefix() + cert.getSerialNumber().toString() + ".crt";
    certCodec.writeCertificate(cert, caCertFileName);

    // first renewed cert
    X509Certificate newCert =
        generateSelfSignedX509Cert(securityConfig, null,
            ZonedDateTime.now().plus(securityConfig.getRenewalGracePeriod()),
            Duration.ofSeconds(certificateLifetime));
    String pemCert = CertificateCodec.getPEMEncodedString(newCert);
    SCMGetCertResponseProto responseProto =
        SCMGetCertResponseProto.newBuilder()
            .setResponseCode(SCMSecurityProtocolProtos
                .SCMGetCertResponseProto.ResponseCode.success)
            .setX509Certificate(pemCert)
            .setX509CACertificate(pemCert)
            .build();
    SCMSecurityProtocolClientSideTranslatorPB scmClient =
        mock(SCMSecurityProtocolClientSideTranslatorPB.class);
    when(scmClient.getOMCertChain(any(), anyString()))
        .thenReturn(responseProto);

    try (OMCertificateClient client = new OMCertificateClient(
        securityConfig, scmClient, omStorage, omInfo, "", scmId, null, null)) {
      client.init();

      // create Ozone Manager instance, it will start the monitor task
      conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");
      om = OzoneManager.createOm(conf);
      om.setScmTopologyClient(new ScmTopologyClient(scmBlockClient));
      om.setCertClient(client);

      // check after renew, client will have the new cert ID
      String id1 = newCert.getSerialNumber().toString();
      GenericTestUtils.waitFor(() ->
              id1.equals(client.getCertificate().getSerialNumber().toString()),
          1000, certificateLifetime * 1000);

      // test the second time certificate rotation
      // second renewed cert
      newCert = generateSelfSignedX509Cert(securityConfig, null, null, Duration.ofSeconds(certificateLifetime));
      pemCert = CertificateCodec.getPEMEncodedString(newCert);
      responseProto = SCMGetCertResponseProto.newBuilder()
          .setResponseCode(SCMSecurityProtocolProtos
              .SCMGetCertResponseProto.ResponseCode.success)
          .setX509Certificate(pemCert)
          .setX509CACertificate(pemCert)
          .build();
      when(scmClient.getOMCertChain(any(), anyString()))
          .thenReturn(responseProto);
      String id2 = newCert.getSerialNumber().toString();

      // check after renew, client will have the new cert ID
      GenericTestUtils.waitFor(() ->
              id2.equals(client.getCertificate().getSerialNumber().toString()),
          1000, certificateLifetime * 1000);
    }
  }

  /**
   * Test unexpected SCMGetCertResponseProto returned from SCM.
   */
  @Test
  void testCertificateRotationRecoverableFailure() throws Exception {
    LogCapturer omLogs = LogCapturer.captureLogs(OMCertificateClient.class);
    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId(clusterId);
    omStorage.setOmId(omId);
    OzoneManager.setTestSecureOmFlag(true);

    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateCodec certCodec = new CertificateCodec(securityConfig, "om");

    // save first cert
    final int certificateLifetime = 20; // seconds
    KeyStorage keyStorage = new KeyStorage(securityConfig, "om");
    X509Certificate certHolder = generateSelfSignedX509Cert(securityConfig,
        keyStorage.readKeyPair(), null, Duration.ofSeconds(certificateLifetime));
    String certId = certHolder.getSerialNumber().toString();
    certCodec.writeCertificate(certHolder);
    omStorage.setOmCertSerialId(certId);
    omStorage.forceInitialize();

    // prepare a mocked scmClient to certificate signing
    SCMSecurityProtocolClientSideTranslatorPB scmClient =
        mock(SCMSecurityProtocolClientSideTranslatorPB.class);

    Duration gracePeriod = securityConfig.getRenewalGracePeriod();
    X509Certificate newCertHolder = generateSelfSignedX509Cert(
        securityConfig, null,
        ZonedDateTime.now().plus(gracePeriod),
        Duration.ofSeconds(certificateLifetime));
    String pemCert = CertificateCodec.getPEMEncodedString(newCertHolder);
    // provide an invalid SCMGetCertResponseProto. Without
    // setX509CACertificate(pemCert), signAndStoreCert will throw exception.
    SCMSecurityProtocolProtos.SCMGetCertResponseProto responseProto =
        SCMSecurityProtocolProtos.SCMGetCertResponseProto
            .newBuilder().setResponseCode(SCMSecurityProtocolProtos
                .SCMGetCertResponseProto.ResponseCode.success)
            .setX509Certificate(pemCert)
            .build();
    when(scmClient.getOMCertChain(any(), anyString()))
        .thenReturn(responseProto);

    try (OMCertificateClient client =
        new OMCertificateClient(
            securityConfig, scmClient, omStorage, omInfo, "", scmId, null, null)
    ) {
      client.init();


      // check that new cert ID should not equal to current cert ID
      String certId1 = newCertHolder.getSerialNumber().toString();
      assertNotEquals(certId1,
          client.getCertificate().getSerialNumber().toString());

      // certificate failed to renew, client still hold the old expired cert.
      Thread.sleep(certificateLifetime * 1000);
      assertEquals(certId,
          client.getCertificate().getSerialNumber().toString());
      assertThrows(CertificateExpiredException.class,
          () -> client.getCertificate().checkValidity());
      assertThat(omLogs.getOutput())
          .contains("Error while signing and storing SCM signed certificate.");

      // provide a new valid SCMGetCertResponseProto
      newCertHolder = generateSelfSignedX509Cert(securityConfig, null, null,
          Duration.ofSeconds(certificateLifetime));
      pemCert = CertificateCodec.getPEMEncodedString(newCertHolder);
      responseProto = SCMSecurityProtocolProtos.SCMGetCertResponseProto
          .newBuilder().setResponseCode(SCMSecurityProtocolProtos
              .SCMGetCertResponseProto.ResponseCode.success)
          .setX509Certificate(pemCert)
          .setX509CACertificate(pemCert)
          .build();
      when(scmClient.getOMCertChain(any(), anyString()))
          .thenReturn(responseProto);
      String certId2 = newCertHolder.getSerialNumber().toString();

      // check after renew, client will have the new cert ID
      GenericTestUtils.waitFor(() -> {
        String newCertId = client.getCertificate().getSerialNumber().toString();
        return newCertId.equals(certId2);
      }, 1000, certificateLifetime * 1000);
    }
  }

  /**
   * Test the directory rollback failure case.
   */
  @Test
  void testCertificateRotationUnRecoverableFailure() throws Exception {
    ExitUtils.disableSystemExit();
    ExitUtil.disableSystemExit();
    LogCapturer certClientLogs = LogCapturer.captureLogs(OMCertificateClient.class);
    LogCapturer exitUtilLog = LogCapturer.captureLogs(ExitUtil.class);


    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId(clusterId);
    omStorage.setOmId(omId);
    OzoneManager.setTestSecureOmFlag(true);

    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateCodec certCodec = new CertificateCodec(securityConfig, "om");
    try (OMCertificateClient client =
             new OMCertificateClient(securityConfig, null, omStorage, omInfo, "", scmId, null, null)
    ) {
      client.init();

      // save first cert
      final int certificateLifetime = 20; // seconds
      KeyPair newKeyPair = new KeyPair(client.getPublicKey(), client.getPrivateKey());
      X509Certificate newCert =
          generateSelfSignedX509Cert(securityConfig, newKeyPair, null, Duration.ofSeconds(certificateLifetime));
      String certId = newCert.getSerialNumber().toString();
      certCodec.writeCertificate(newCert);
      omStorage.setOmCertSerialId(certId);
      omStorage.forceInitialize();

      // create Ozone Manager instance, it will start the monitor task
      conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");
      om = OzoneManager.createOm(conf);
      om.getCertificateClient().close();
      om.setScmTopologyClient(new ScmTopologyClient(scmBlockClient));

      OMCertificateClient mockClient = new OMCertificateClient(securityConfig, null, omStorage, omInfo,
          om.getOMServiceId(), scmId, null, om::terminateOM) {

        @Override
        public Duration timeBeforeExpiryGracePeriod(X509Certificate certificate) {
          return Duration.ZERO;
        }

        @Override
        public String renewAndStoreKeyAndCertificate(boolean force) throws CertificateException {
          throw new CertificateException("renewAndStoreKeyAndCert failed", ROLLBACK_ERROR);
        }
      };
      om.setCertClient(mockClient);

      // check error message during renew
      GenericTestUtils.waitFor(() ->
              certClientLogs.getOutput().contains("Failed to rollback key and cert after an unsuccessful renew try."),
          1000, certificateLifetime * 1000);

      GenericTestUtils.waitFor(() -> exitUtilLog.getOutput().contains("Exiting with status 1: ExitException"),
          100, 5000);
    }
  }

  /**
   * Tests delegation token renewal after a secret key rotation.
   */
  @Test
  void testDelegationTokenRenewCrossSecretKeyRotation() throws Exception {
    initSCM();
    try {
      scm = HddsTestUtils.getScmSimple(conf);
      scm.start();

      // Setup secure OM for start.
      final int certLifetime = 40 * 1000; // 40s
      OzoneConfiguration newConf = new OzoneConfiguration(conf);
      newConf.set(HDDS_X509_DEFAULT_DURATION,
          Duration.ofMillis(certLifetime).toString());
      newConf.set(HDDS_X509_RENEW_GRACE_DURATION,
          Duration.ofMillis(certLifetime - 15 * 1000).toString());
      newConf.setLong(OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
          certLifetime - 20 * 1000);

      setupOm(newConf);
      OzoneManager.setTestSecureOmFlag(true);

      CertificateClientTestImpl certClient =
          new CertificateClientTestImpl(newConf, true);
      // Start OM
      om.setCertClient(certClient);
      om.setScmTopologyClient(new ScmTopologyClient(scmBlockClient));
      SecretKeyTestClient secretKeyClient = new SecretKeyTestClient();
      ManagedSecretKey secretKey1 = secretKeyClient.getCurrentSecretKey();
      om.setSecretKeyClient(secretKeyClient);
      om.start();
      GenericTestUtils.waitFor(() -> om.isLeaderReady(), 100, 10000);

      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(newConf, ugi, null),
          RandomStringUtils.secure().nextAscii(5));

      // Since client is already connected get a delegation token
      Token<OzoneTokenIdentifier> token1 = omClient.getDelegationToken(
          new Text("om"));

      // Check if token is of right kind and renewer is running om instance
      assertNotNull(token1);
      assertEquals("OzoneToken", token1.getKind().toString());
      assertEquals(SecurityUtil.buildTokenService(
          om.getNodeDetails().getRpcAddress()).toString(),
          token1.getService().toString());
      assertEquals(secretKey1.getId().toString(), token1.decodeIdentifier().getSecretKeyId());

      // Renew delegation token
      long expiryTime = omClient.renewDelegationToken(token1);
      assertThat(expiryTime).isGreaterThan(0);

      // Rotate secret key
      secretKeyClient.rotate();
      ManagedSecretKey secretKey2 = secretKeyClient.getCurrentSecretKey();
      assertNotEquals(secretKey1.getId(), secretKey2.getId());
      // Get a new delegation token
      Token<OzoneTokenIdentifier> token2 = omClient.getDelegationToken(
          new Text("om"));
      assertEquals(secretKey2.getId().toString(), token2.decodeIdentifier().getSecretKeyId());

      // Because old secret key is still valid, so renew old token will succeed
      expiryTime = omClient.renewDelegationToken(token1);
      assertThat(expiryTime)
          .isGreaterThan(0)
          .isLessThan(secretKey2.getExpiryTime().toEpochMilli());
    } finally {
      if (scm != null) {
        scm.stop();
      }
      IOUtils.closeQuietly(om);
    }
  }

  /**
   * Test functionality to get SCM signed certificate for OM.
   */
  @Test
  @Unhealthy("HDDS-8764")
  void testOMGrpcServerCertificateRenew() throws Exception {
    initSCM();
    try {
      OzoneManager.setTestSecureOmFlag(false);
      scm = HddsTestUtils.getScmSimple(conf);
      scm.start();

      conf.set(OZONE_METADATA_DIRS, omMetaDirPath.toString());
      int certLifetime = 30; // second
      conf.set(HDDS_X509_DEFAULT_DURATION,
          Duration.ofSeconds(certLifetime).toString());
      conf.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 2);

      // initialize OmStorage, save om Cert and CA Certs to disk
      OMStorage omStore = new OMStorage(conf);
      omStore.setClusterId(clusterId);
      omStore.setOmId(omId);

      // Prepare the certificates for OM before OM start
      SecurityConfig securityConfig = new SecurityConfig(conf);
      CertificateClient scmCertClient = scm.getScmCertificateClient();
      CertificateCodec certCodec = new CertificateCodec(securityConfig, "om");
      X509Certificate scmCert = scmCertClient.getCertificate();
      X509Certificate rootCert = scmCertClient.getCACertificate();
      X509Certificate cert = signX509Cert(securityConfig, keyPair, new KeyPair(scmCertClient.getPublicKey(),
          scmCertClient.getPrivateKey()), scmCert, "om_cert", clusterId);
      String certId = cert.getSerialNumber().toString();
      certCodec.writeCertificate(cert);
      certCodec.writeCertificate(scmCert, String.format(DefaultCertificateClient.CERT_FILE_NAME_FORMAT,
          CAType.SUBORDINATE.getFileNamePrefix() + scmCert.getSerialNumber().toString()));
      certCodec.writeCertificate(scmCertClient.getCACertificate(),
          String.format(DefaultCertificateClient.CERT_FILE_NAME_FORMAT,
              CAType.ROOT.getFileNamePrefix() + rootCert.getSerialNumber().toString()));
      omStore.setOmCertSerialId(certId);
      omStore.initialize();

      conf.setBoolean(HDDS_GRPC_TLS_ENABLED, true);
      conf.setBoolean(OZONE_OM_S3_GPRC_SERVER_ENABLED, true);
      conf.setBoolean(HddsConfigKeys.HDDS_GRPC_TLS_TEST_CERT, true);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      // In this process, SCM has already logged in using Kerberos. So pass a
      // specific UGI to DefaultCertificateClient and OzoneManager to avoid
      // conflict with SCM.
      OzoneManager.setUgi(ugi);
      om = OzoneManager.createOm(conf);
      om.start();

      CertificateClient omCertClient = om.getCertificateClient();
      X509Certificate omCert = omCertClient.getCertificate();
      X509Certificate caCert = omCertClient.getRootCACertificate();
      X509Certificate rootCaCert = omCertClient.getRootCACertificate();
      List<X509Certificate> certList = new ArrayList<>();
      certList.add(caCert);
      certList.add(rootCaCert);
      // set certificates in GrpcOmTransport
      GrpcOmTransport.setCaCerts(certList);

      GenericTestUtils.waitFor(() -> om.isLeaderReady(), 500, 10000);
      String transportCls = GrpcOmTransportFactory.class.getName();
      conf.set(OZONE_OM_TRANSPORT_CLASS, transportCls);
      try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {

        ServiceInfoEx serviceInfoEx = client.getObjectStore()
            .getClientProxy().getOzoneManagerClient().getServiceInfo();
        assertEquals(CertificateCodec.getPEMEncodedString(caCert), serviceInfoEx.getCaCertificate());

        // Wait for OM certificate to renewed
        GenericTestUtils.waitFor(() ->
                !omCert.getSerialNumber().toString().equals(
                    omCertClient.getCertificate().getSerialNumber().toString()),
            500, certLifetime * 1000);

        // rerun the command using old client, it should succeed
        serviceInfoEx = client.getObjectStore()
            .getClientProxy().getOzoneManagerClient().getServiceInfo();
        assertEquals(CertificateCodec.getPEMEncodedString(caCert), serviceInfoEx.getCaCertificate());
      }

      // get new client, it should succeed.
      OzoneClient client1 = OzoneClientFactory.getRpcClient(conf);
      client1.close();


      // Wait for old OM certificate to expire
      GenericTestUtils.waitFor(() -> omCert.getNotAfter().before(new Date()),
          500, certLifetime * 1000);
      // get new client, it should succeed too.
      OzoneClient client2 = OzoneClientFactory.getRpcClient(conf);
      client2.close();
    } finally {
      OzoneManager.setUgi(null);
      GrpcOmTransport.setCaCerts(null);
    }
  }

  void validateCertificate(X509Certificate cert) throws Exception {
    Matcher m = Pattern.compile(".*CN=([^,]*).*").matcher(cert.getIssuerDN().getName());
    String cn = "";
    if (m.matches()) {
      cn = m.group(1);
    }
    String hostName = InetAddress.getLocalHost().getHostName();

    // Subject name should be om login user in real world but in this test
    // UGI has scm user context.
    assertThat(cn).contains(SCM_SUB_CA);
    assertThat(cn).contains(hostName);

    LocalDate today = ZonedDateTime.now().toLocalDate();
    Date invalidDate;

    // Make sure the end date is honored.
    invalidDate = java.sql.Date.valueOf(today.plus(1, ChronoUnit.DAYS));
    assertTrue(cert.getNotAfter().after(invalidDate));

    invalidDate = java.sql.Date.valueOf(today.plus(400, ChronoUnit.DAYS));
    assertTrue(cert.getNotAfter().before(invalidDate));

    assertThat(cert.getSubjectDN().toString()).contains(scmId);
    assertThat(cert.getSubjectDN().toString()).contains(clusterId);

    assertThat(cert.getIssuerDN().toString()).contains(scmId);
    assertThat(cert.getIssuerDN().toString()).contains(clusterId);

    // Verify that certificate matches the public key.
    assertEquals(cert.getPublicKey(), om.getCertificateClient().getPublicKey());
  }

  private void initializeOmStorage(OMStorage omStorage) throws IOException {
    if (omStorage.getState() == Storage.StorageState.INITIALIZED) {
      return;
    }
    omStorage.setClusterId(clusterId);
    omStorage.setOmId(omId);
    // Initialize ozone certificate client if security is enabled.
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      OzoneManager.initializeSecurity(conf, omStorage, scmId);
    }
    omStorage.initialize();
  }

  private static X509Certificate generateSelfSignedX509Cert(
      SecurityConfig conf, KeyPair keyPair, ZonedDateTime startDate,
      Duration certLifetime) throws Exception {
    if (keyPair == null) {
      keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    }
    ZonedDateTime start = startDate == null ? ZonedDateTime.now() : startDate;
    ZonedDateTime end = start.plus(certLifetime);
    return SelfSignedCertificate.newBuilder()
        .setBeginDate(start)
        .setEndDate(end)
        .setClusterID("cluster")
        .setKey(keyPair)
        .setSubject("localhost")
        .setConfiguration(conf)
        .setScmID("test")
        .build();
  }

  private static X509Certificate signX509Cert(
      SecurityConfig conf, KeyPair keyPair, KeyPair rootKeyPair,
      X509Certificate rootCert, String subject, String clusterId
  ) throws Exception {
    // Generate normal certificate, signed by RootCA certificate
    DefaultApprover approver = new DefaultApprover(new DefaultProfile(), conf);

    CertificateSignRequest.Builder csrBuilder =
        new CertificateSignRequest.Builder();
    // Get host name.
    csrBuilder.setKey(keyPair)
        .setConfiguration(conf)
        .setScmID("test")
        .setClusterID(clusterId)
        .setSubject(subject)
        .setDigitalSignature(true)
        .setDigitalEncryption(true);

    addIpAndDnsDataToBuilder(csrBuilder);
    ZonedDateTime start = ZonedDateTime.now();
    Duration certDuration = conf.getDefaultCertDuration();
    //TODO: generateCSR!
    return approver.sign(conf, rootKeyPair.getPrivate(), rootCert,
            Date.from(start.toInstant()),
            Date.from(start.plus(certDuration).toInstant()),
            csrBuilder.build().generateCSR(), "test", clusterId,
            String.valueOf(System.nanoTime()));
  }

  private static void addIpAndDnsDataToBuilder(
      CertificateSignRequest.Builder csrBuilder) throws IOException {
    DomainValidator validator = DomainValidator.getInstance();
    // Add all valid ips.
    List<InetAddress> inetAddresses =
        OzoneSecurityUtil.getValidInetsForCurrentHost();
    csrBuilder.addInetAddresses(inetAddresses, validator);
  }
}

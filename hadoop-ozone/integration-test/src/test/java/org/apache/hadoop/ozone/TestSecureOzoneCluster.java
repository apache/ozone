/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.validator.routines.DomainValidator;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.DefaultConfigManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.HASecurityUtils;
import org.apache.hadoop.hdds.scm.ha.SCMHANodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyManager;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultApprover;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCAServer;
import org.apache.hadoop.hdds.security.x509.certificate.client.DefaultCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
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
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_ACK_TIMEOUT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_CHECK_INTERNAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DEFAULT_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClient;
import static org.apache.hadoop.net.ServerSocketUtil.getPort;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_SUB_CA;
import static org.apache.hadoop.ozone.om.OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_EXPIRED;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;

import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ozone.test.tag.Unhealthy;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.util.ExitUtils;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateHolder;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.INFO;

/**
 * Test class to for security enabled Ozone cluster.
 */
@Timeout(80)
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
  public void init() {
    try {
      conf = new OzoneConfiguration();
      conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");

      conf.setInt(OZONE_SCM_CLIENT_PORT_KEY,
          getPort(OZONE_SCM_CLIENT_PORT_DEFAULT, 100));
      conf.setInt(OZONE_SCM_DATANODE_PORT_KEY,
          getPort(OZONE_SCM_DATANODE_PORT_DEFAULT, 100));
      conf.setInt(OZONE_SCM_BLOCK_CLIENT_PORT_KEY,
          getPort(OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT, 100));
      conf.setInt(OZONE_SCM_SECURITY_SERVICE_PORT_KEY,
          getPort(OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT, 100));
      // use the same base ports as MiniOzoneHACluster
      conf.setInt(OZONE_SCM_RATIS_PORT_KEY, getPort(1200, 100));
      conf.setInt(OZONE_SCM_GRPC_PORT_KEY, getPort(1201, 100));
      conf.set(OZONE_OM_ADDRESS_KEY,
          InetAddress.getLocalHost().getCanonicalHostName() + ":1202");
      conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, false);

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
  public void stop() {
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
    DefaultConfigManager.clearDefaultConfigs();
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
  public void testSecureScmStartupSuccess() throws Exception {

    initSCM();
    scm = HddsTestUtils.getScmSimple(conf);
    //Reads the SCM Info from SCM instance
    ScmInfo scmInfo = scm.getClientProtocolServer().getScmInfo();
    assertEquals(clusterId, scmInfo.getClusterId());
    assertEquals(scmId, scmInfo.getScmId());
    assertEquals(2, scm.getScmCertificateClient().getTrustChain().size());
  }

  @Test
  public void testSCMSecurityProtocol() throws Exception {

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
        assertTrue(securityException.getMessage()
            .contains("Certificate not found"));
      }

      // Case 2: User without Kerberos credentials should fail.
      ugi = UserGroupInformation.createRemoteUser("test");
      ugi.setAuthenticationMethod(AuthMethod.TOKEN);
      try (SCMSecurityProtocolClientSideTranslatorPB securityClient =
          getScmSecurityClient(conf, ugi)) {

        String cannotAuthMessage = "Client cannot authenticate via:[KERBEROS]";
        IOException ioException = assertThrows(IOException.class,
            securityClient::getCACertificate);
        assertTrue(ioException.getMessage().contains(cannotAuthMessage));
        ioException = assertThrows(IOException.class,
            () -> securityClient.getCertificate("1"));
        assertTrue(ioException.getMessage().contains(cannotAuthMessage));
      }
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  @Test
  public void testAdminAccessControlException() throws Exception {
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
      assertTrue(ioException.getMessage().contains("Access denied"));

      // Case 2: User without Kerberos credentials should fail.
      ugi = UserGroupInformation.createRemoteUser("test");
      ugi.setAuthenticationMethod(AuthMethod.TOKEN);
      scmRpcClient =
          HAUtils.getScmContainerClient(conf, ugi);

      String cannotAuthMessage = "Client cannot authenticate via:[KERBEROS]";
      ioException = assertThrows(IOException.class,
          scmRpcClient::forceExitSafeMode);
      assertTrue(ioException.getMessage().contains(cannotAuthMessage));
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  @Test
  public void testSecretManagerInitializedNonHASCM() throws Exception {
    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    initSCM();
    scm = HddsTestUtils.getScmSimple(conf);
    //Reads the SCM Info from SCM instance
    try {
      scm.start();

      SecretKeyManager secretKeyManager = scm.getSecretKeyManager();
      boolean inSafeMode = scm.getScmSafeModeManager().getInSafeMode();
      Assert.assertTrue(!SCMHAUtils.isSCMHAEnabled(conf));
      Assert.assertTrue(inSafeMode);
      Assert.assertTrue(secretKeyManager.isInitialized());
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

    DefaultCAServer.setTestSecureFlag(true);
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    scmStore.setClusterId(clusterId);
    scmStore.setScmId(scmId);
    HASecurityUtils.initializeSecurity(scmStore, conf,
        InetAddress.getLocalHost().getHostName(), true);
    scmStore.setPrimaryScmNodeId(scmId);
    // writes the version file properties
    scmStore.initialize();
    if (SCMHAUtils.isSCMHAEnabled(conf)) {
      SCMRatisServerImpl.initialize(clusterId, scmId,
          SCMHANodeDetails.loadSCMHAConfig(conf, scmStore)
                  .getLocalNodeDetails(), conf);
    }

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
  public void testSecureScmStartupFailure() throws Exception {
    initSCM();
    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, "");
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");

    IOException ioException = assertThrows(IOException.class,
        () -> HddsTestUtils.getScmSimple(conf));
    assertTrue(ioException.getMessage()
        .contains("Running in secure mode, but config doesn't have a keytab"));

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
    assertTrue(kerberosAuthException.getMessage()
        .contains("failure to login: for principal:"));

    String invalidValue = "OAuth2";
    conf.set(HADOOP_SECURITY_AUTHENTICATION, invalidValue);
    IllegalArgumentException argumentException =
        assertThrows(IllegalArgumentException.class, test::call);
    assertTrue(argumentException.getMessage()
        .contains("Invalid attribute value for " +
                HADOOP_SECURITY_AUTHENTICATION + " of " + invalidValue));

    conf.set(HADOOP_SECURITY_AUTHENTICATION, "KERBEROS_SSL");
    AuthenticationException authException = assertThrows(
        AuthenticationException.class,
        test::call);
    assertTrue(authException.getMessage()
        .contains("KERBEROS_SSL authentication method not"));
  }

  /**
   * Tests the secure om Initialization Failure.
   */
  @Test
  public void testSecureOMInitializationFailure() throws Exception {
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
   * Tests the secure om Initialization success.
   */
  @Test
  public void testSecureOmInitializationSuccess() throws Exception {
    initSCM();
    // Create a secure SCM instance as om client will connect to it
    scm = HddsTestUtils.getScmSimple(conf);
    LogCapturer logs = LogCapturer.captureLogs(OzoneManager.getLogger());
    GenericTestUtils.setLogLevel(OzoneManager.getLogger(), INFO);

    try {
      scm.start();
      setupOm(conf);
      om.start();
    } catch (Exception ex) {
      // Expects timeout failure from scmClient in om but om user login via
      // kerberos should succeed.
      assertTrue(logs.getOutput().contains("Ozone Manager login successful"));
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  @Test
  public void testAccessControlExceptionOnClient() throws Exception {
    initSCM();
    LogCapturer logs = LogCapturer.captureLogs(OzoneManager.getLogger());
    GenericTestUtils.setLogLevel(OzoneManager.getLogger(), INFO);
    try {
      // Create a secure SCM instance as om client will connect to it
      scm = HddsTestUtils.getScmSimple(conf);
      scm.start();

      setupOm(conf);
      om.setCertClient(new CertificateClientTestImpl(conf));
      om.start();
    } catch (Exception ex) {
      // Expects timeout failure from scmClient in om but om user login via
      // kerberos should succeed.
      assertTrue(logs.getOutput().contains("Ozone Manager login successful"));
    }
    UserGroupInformation ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
            testUserPrincipal, testUserKeytab.getCanonicalPath());
    ugi.setAuthenticationMethod(KERBEROS);
    OzoneManagerProtocolClientSideTranslatorPB secureClient =
        new OzoneManagerProtocolClientSideTranslatorPB(
            OmTransportFactory.create(conf, ugi, null),
            ClientId.randomId().toString());
    try {
      secureClient.createVolume(
          new OmVolumeArgs.Builder().setVolume("vol1")
              .setOwnerName("owner1")
              .setAdminName("admin")
              .build());
    } catch (IOException ex) {
      fail("Secure client should be able to create volume.");
    }

    ugi = UserGroupInformation.createUserForTesting(
        "testuser1", new String[] {"test"});

    OzoneManagerProtocolClientSideTranslatorPB unsecureClient =
        new OzoneManagerProtocolClientSideTranslatorPB(
            OmTransportFactory.create(conf, ugi, null),
            ClientId.randomId().toString());
    String exMessage = "org.apache.hadoop.security.AccessControlException: " +
        "Client cannot authenticate via:[TOKEN, KERBEROS]";
    logs = LogCapturer.captureLogs(Client.LOG);
    IOException ioException = assertThrows(IOException.class,
        () -> unsecureClient.listAllVolumes(null, null, 0));
    assertTrue(ioException.getMessage().contains(exMessage));
    assertEquals(1, StringUtils.countMatches(logs.getOutput(), exMessage),
        "There should be no retry on AccessControlException");
  }

  private void generateKeyPair() throws Exception {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(securityConfig);
    keyPair = keyGenerator.generateKey();
    KeyCodec pemWriter = new KeyCodec(securityConfig, COMPONENT);
    pemWriter.writeKey(keyPair, true);
  }

  /**
   * Tests delegation token renewal.
   */
  @Test
  public void testDelegationTokenRenewal() throws Exception {
    GenericTestUtils
        .setLogLevel(LoggerFactory.getLogger(Server.class.getName()), INFO);
    LogCapturer omLogs = LogCapturer.captureLogs(OzoneManager.getLogger());

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
      om.start();

      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(conf, ugi, null),
          RandomStringUtils.randomAscii(5));

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
      assertTrue(expiryTime > 0);
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
      assertTrue(ex.getMessage().contains("Delegation token renewal failed"));
      assertTrue(omLogs.getOutput().contains(" with non-matching " +
          "renewer randomService"));
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
      assertTrue(ex.getMessage().contains("Delegation token renewal failed"));
      assertTrue(omLogs.getOutput().contains("can't be found in cache"));
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
  public void testGetSetRevokeS3Secret() throws Exception {
    initSCM();
    try {
      scm = HddsTestUtils.getScmSimple(conf);
      scm.start();

      // Setup secure OM for start
      setupOm(conf);
      // Start OM
      om.setCertClient(new CertificateClientTestImpl(conf));
      om.start();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String username = ugi.getUserName();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(conf, ugi, null),
          RandomStringUtils.randomAscii(5));

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
          RandomStringUtils.randomAscii(5));

      try {
        omClientNonAdmin.getS3Secret("HADOOP/JOHN");
        // Expected to fail because current ugi isn't an admin
        fail("non-admin getS3Secret didn't fail as intended");
      } catch (IOException ex) {
        GenericTestUtils.assertExceptionContains("USER_MISMATCH", ex);
      }

      try {
        omClientNonAdmin.revokeS3Secret("HADOOP/DOE");
        // Expected to fail because current ugi isn't an admin
        fail("non-admin revokeS3Secret didn't fail as intended");
      } catch (IOException ex) {
        GenericTestUtils.assertExceptionContains("USER_MISMATCH", ex);
      }

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
  public void testSecureOmReInit() throws Exception {
    LogCapturer omLogs =
        LogCapturer.captureLogs(OMCertificateClient.LOG);
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
      assertFalse(omLogs.getOutput().contains("Init response: GETCERT"));
      assertFalse(omLogs.getOutput().contains("Successfully stored " +
          "SCM signed certificate"));

      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
      conf.setBoolean(OZONE_OM_S3_GPRC_SERVER_ENABLED, true);

      OzoneManager.omInit(conf);
      om.stop();
      om = OzoneManager.createOm(conf);

      assertNotNull(om.getCertificateClient());
      assertNotNull(om.getCertificateClient().getPublicKey());
      assertNotNull(om.getCertificateClient().getPrivateKey());
      assertNotNull(om.getCertificateClient().getCertificate());
      assertTrue(omLogs.getOutput().contains("Init response: GETCERT"));
      assertTrue(omLogs.getOutput().contains("Successfully stored " +
          "OM signed certificate"));
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
  public void testSecureOmInitSuccess() throws Exception {
    LogCapturer omLogs =
        LogCapturer.captureLogs(OMCertificateClient.LOG);
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
      assertTrue(omLogs.getOutput().contains("Init response: GETCERT"));
      assertTrue(omLogs.getOutput().contains("Successfully stored " +
          "OM signed certificate"));
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
  public void testCertificateRotation() throws Exception {
    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId(clusterId);
    omStorage.setOmId(omId);
    OzoneManager.setTestSecureOmFlag(true);

    SecurityConfig securityConfig = new SecurityConfig(conf);

    // save first cert
    final int certificateLifetime = 20; // seconds
    KeyCodec keyCodec =
        new KeyCodec(securityConfig, securityConfig.getKeyLocation("om"));
    X509CertificateHolder certHolder = generateX509CertHolder(securityConfig,
        new KeyPair(keyCodec.readPublicKey(), keyCodec.readPrivateKey()),
        null, Duration.ofSeconds(certificateLifetime));
    String certId = certHolder.getSerialNumber().toString();
    omStorage.setOmCertSerialId(certId);
    omStorage.forceInitialize();
    CertificateCodec certCodec = new CertificateCodec(securityConfig, "om");
    certCodec.writeCertificate(certHolder);
    String caCertFileName = CAType.ROOT.getFileNamePrefix()
        + certHolder.getSerialNumber().toString() + ".crt";
    certCodec.writeCertificate(certHolder, caCertFileName);

    // first renewed cert
    X509CertificateHolder newCertHolder =
        generateX509CertHolder(securityConfig, null,
            LocalDateTime.now().plus(securityConfig.getRenewalGracePeriod()),
            Duration.ofSeconds(certificateLifetime));
    String pemCert = CertificateCodec.getPEMEncodedString(newCertHolder);
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
      om.setCertClient(client);

      // check after renew, client will have the new cert ID
      String id1 = newCertHolder.getSerialNumber().toString();
      GenericTestUtils.waitFor(() ->
              id1.equals(client.getCertificate().getSerialNumber().toString()),
          1000, certificateLifetime * 1000);

      // test the second time certificate rotation
      // second renewed cert
      newCertHolder = generateX509CertHolder(securityConfig,
          null, null, Duration.ofSeconds(certificateLifetime));
      pemCert = CertificateCodec.getPEMEncodedString(newCertHolder);
      responseProto = SCMGetCertResponseProto.newBuilder()
          .setResponseCode(SCMSecurityProtocolProtos
              .SCMGetCertResponseProto.ResponseCode.success)
          .setX509Certificate(pemCert)
          .setX509CACertificate(pemCert)
          .build();
      when(scmClient.getOMCertChain(any(), anyString()))
          .thenReturn(responseProto);
      String id2 = newCertHolder.getSerialNumber().toString();

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
  public void testCertificateRotationRecoverableFailure() throws Exception {
    LogCapturer omLogs = LogCapturer.captureLogs(OMCertificateClient.LOG);
    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId(clusterId);
    omStorage.setOmId(omId);
    OzoneManager.setTestSecureOmFlag(true);

    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateCodec certCodec = new CertificateCodec(securityConfig, "om");

    // save first cert
    final int certificateLifetime = 20; // seconds
    KeyCodec keyCodec =
        new KeyCodec(securityConfig, securityConfig.getKeyLocation("om"));
    X509CertificateHolder certHolder = generateX509CertHolder(securityConfig,
        new KeyPair(keyCodec.readPublicKey(), keyCodec.readPrivateKey()),
        null, Duration.ofSeconds(certificateLifetime));
    String certId = certHolder.getSerialNumber().toString();
    certCodec.writeCertificate(certHolder);
    omStorage.setOmCertSerialId(certId);
    omStorage.forceInitialize();

    // prepare a mocked scmClient to certificate signing
    SCMSecurityProtocolClientSideTranslatorPB scmClient =
        mock(SCMSecurityProtocolClientSideTranslatorPB.class);

    Duration gracePeriod = securityConfig.getRenewalGracePeriod();
    X509CertificateHolder newCertHolder = generateX509CertHolder(
        securityConfig, null,
        LocalDateTime.now().plus(gracePeriod),
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
      assertTrue(omLogs.getOutput().contains(
          "Error while signing and storing SCM signed certificate."));

      // provide a new valid SCMGetCertResponseProto
      newCertHolder = generateX509CertHolder(securityConfig, null, null,
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
  @Unhealthy("Run it locally since it will terminate the process.")
  public void testCertificateRotationUnRecoverableFailure() throws Exception {
    LogCapturer omLogs = LogCapturer.captureLogs(OzoneManager.getLogger());
    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId(clusterId);
    omStorage.setOmId(omId);
    OzoneManager.setTestSecureOmFlag(true);

    SecurityConfig securityConfig = new SecurityConfig(conf);
    CertificateCodec certCodec = new CertificateCodec(securityConfig, "om");
    try (OMCertificateClient client =
        new OMCertificateClient(
            securityConfig, null, omStorage, omInfo, "", scmId, null, null)
    ) {
      client.init();

      // save first cert
      final int certificateLifetime = 20; // seconds
      X509CertificateHolder certHolder = generateX509CertHolder(securityConfig,
          new KeyPair(client.getPublicKey(), client.getPrivateKey()),
          null, Duration.ofSeconds(certificateLifetime));
      String certId = certHolder.getSerialNumber().toString();
      certCodec.writeCertificate(certHolder);
      omStorage.setOmCertSerialId(certId);
      omStorage.forceInitialize();

      // second cert as renew response
      X509CertificateHolder newCertHolder = generateX509CertHolder(
          securityConfig, null,
          null, Duration.ofSeconds(certificateLifetime));
      DNCertificateClient mockClient = mock(DNCertificateClient.class);
      when(mockClient.getCertificate()).thenReturn(
          CertificateCodec.getX509Certificate(newCertHolder));
      when(mockClient.timeBeforeExpiryGracePeriod(any()))
          .thenReturn(Duration.ZERO);
      when(mockClient.renewAndStoreKeyAndCertificate(any())).thenThrow(
          new CertificateException("renewAndStoreKeyAndCert failed ",
              CertificateException.ErrorCode.ROLLBACK_ERROR));

      // create Ozone Manager instance, it will start the monitor task
      conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");
      om = OzoneManager.createOm(conf);
      om.setCertClient(mockClient);

      // check error message during renew
      GenericTestUtils.waitFor(() -> omLogs.getOutput().contains(
              "OzoneManage shutdown because certificate rollback failure."),
          1000, certificateLifetime * 1000);
    }
  }

  /**
   * Tests delegation token renewal after a certificate renew.
   */
  @Test
  public void testDelegationTokenRenewCrossCertificateRenew() throws Exception {
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
      X509Certificate omCert = certClient.getCertificate();
      String omCertId1 = omCert.getSerialNumber().toString();
      // Start OM
      om.setCertClient(certClient);
      om.start();
      GenericTestUtils.waitFor(() -> om.isLeaderReady(), 100, 10000);

      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(newConf, ugi, null),
          RandomStringUtils.randomAscii(5));

      // Since client is already connected get a delegation token
      Token<OzoneTokenIdentifier> token1 = omClient.getDelegationToken(
          new Text("om"));

      // Check if token is of right kind and renewer is running om instance
      assertNotNull(token1);
      assertEquals("OzoneToken", token1.getKind().toString());
      assertEquals(SecurityUtil.buildTokenService(
          om.getNodeDetails().getRpcAddress()).toString(),
          token1.getService().toString());
      assertEquals(omCertId1, token1.decodeIdentifier().getOmCertSerialId());

      // Renew delegation token
      long expiryTime = omClient.renewDelegationToken(token1);
      assertTrue(expiryTime > 0);

      // Wait for OM certificate to renew
      LambdaTestUtils.await(certLifetime, 100, () ->
          !StringUtils.equals(token1.decodeIdentifier().getOmCertSerialId(),
              omClient.getDelegationToken(new Text("om"))
                  .decodeIdentifier().getOmCertSerialId()));
      String omCertId2 =
          certClient.getCertificate().getSerialNumber().toString();
      assertNotEquals(omCertId1, omCertId2);
      // Get a new delegation token
      Token<OzoneTokenIdentifier> token2 = omClient.getDelegationToken(
          new Text("om"));
      assertEquals(omCertId2, token2.decodeIdentifier().getOmCertSerialId());

      // Because old certificate is still valid, so renew old token will succeed
      expiryTime = omClient.renewDelegationToken(token1);
      assertTrue(expiryTime > 0);
      assertTrue(new Date(expiryTime).before(omCert.getNotAfter()));
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
  public void testOMGrpcServerCertificateRenew() throws Exception {
    initSCM();
    try {
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
      X509CertificateHolder certHolder =
          generateX509CertHolder(securityConfig, keyPair,
              new KeyPair(scmCertClient.getPublicKey(),
                  scmCertClient.getPrivateKey()),
              scmCert, "om_cert", clusterId);
      String certId = certHolder.getSerialNumber().toString();
      certCodec.writeCertificate(certHolder);
      certCodec.writeCertificate(CertificateCodec.getCertificateHolder(scmCert),
          String.format(DefaultCertificateClient.CERT_FILE_NAME_FORMAT,
          CAType.SUBORDINATE.getFileNamePrefix() +
              scmCert.getSerialNumber().toString()));
      certCodec.writeCertificate(CertificateCodec.getCertificateHolder(
          scmCertClient.getCACertificate()),
          String.format(DefaultCertificateClient.CERT_FILE_NAME_FORMAT,
              CAType.ROOT.getFileNamePrefix() +
                  rootCert.getSerialNumber().toString()));
      omStore.setOmCertSerialId(certId);
      omStore.initialize();

      conf.setBoolean(HDDS_GRPC_TLS_ENABLED, true);
      conf.setBoolean(OZONE_OM_S3_GPRC_SERVER_ENABLED, true);
      conf.setBoolean(HddsConfigKeys.HDDS_GRPC_TLS_TEST_CERT, true);
      OzoneManager.setTestSecureOmFlag(true);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      // In this process, SCM has already login using Kerberos. So pass
      // specific UGI to DefaultCertificateClient and OzoneManager to avoid
      // conflict with SCM procedure.
      OzoneManager.setUgi(ugi);
      om = OzoneManager.createOm(conf);
      om.start();

      CertificateClient omCertClient = om.getCertificateClient();
      X509Certificate omCert = omCertClient.getCertificate();
      X509Certificate caCert = omCertClient.getCACertificate();
      X509Certificate rootCaCert = omCertClient.getRootCACertificate();
      List certList = new ArrayList<>();
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
        assertTrue(serviceInfoEx.getCaCertificate().equals(
            CertificateCodec.getPEMEncodedString(caCert)));

        // Wait for OM certificate to renewed
        GenericTestUtils.waitFor(() ->
                !omCert.getSerialNumber().toString().equals(
                    omCertClient.getCertificate().getSerialNumber().toString()),
            500, certLifetime * 1000);

        // rerun the command using old client, it should succeed
        serviceInfoEx = client.getObjectStore()
            .getClientProxy().getOzoneManagerClient().getServiceInfo();
        assertTrue(serviceInfoEx.getCaCertificate().equals(
            CertificateCodec.getPEMEncodedString(caCert)));
      }

      // get new client, it should succeed.
      try {
        OzoneClient client1 = OzoneClientFactory.getRpcClient(conf);
        client1.close();
      } catch (Exception e) {
        System.out.println("OzoneClientFactory.getRpcClient failed for " +
            e.getMessage());
        fail("Create client should succeed for certificate is renewed");
      }

      // Wait for old OM certificate to expire
      GenericTestUtils.waitFor(() -> omCert.getNotAfter().before(new Date()),
          500, certLifetime * 1000);
      // get new client, it should succeed too.
      try {
        OzoneClient client1 = OzoneClientFactory.getRpcClient(conf);
        client1.close();
      } catch (Exception e) {
        System.out.println("OzoneClientFactory.getRpcClient failed for " +
            e.getMessage());
        fail("Create client should succeed for certificate is renewed");
      }
    } finally {
      OzoneManager.setUgi(null);
      GrpcOmTransport.setCaCerts(null);
    }
  }

  public void validateCertificate(X509Certificate cert) throws Exception {

    // Assert that we indeed have a self signed certificate.
    X500Name x500Issuer = new JcaX509CertificateHolder(cert).getIssuer();
    RDN cn = x500Issuer.getRDNs(BCStyle.CN)[0];
    String hostName = InetAddress.getLocalHost().getHostName();

    // Subject name should be om login user in real world but in this test
    // UGI has scm user context.
    assertTrue(cn.getFirst().getValue().toString().contains(SCM_SUB_CA));
    assertTrue(cn.getFirst().getValue().toString().contains(hostName));

    LocalDate today = LocalDateTime.now().toLocalDate();
    Date invalidDate;

    // Make sure the end date is honored.
    invalidDate = java.sql.Date.valueOf(today.plus(1, ChronoUnit.DAYS));
    assertTrue(cert.getNotAfter().after(invalidDate));

    invalidDate = java.sql.Date.valueOf(today.plus(400, ChronoUnit.DAYS));
    assertTrue(cert.getNotAfter().before(invalidDate));

    assertTrue(cert.getSubjectDN().toString().contains(scmId));
    assertTrue(cert.getSubjectDN().toString().contains(clusterId));

    assertTrue(cn.getFirst().getValue().toString().contains(SCM_SUB_CA));
    assertTrue(cn.getFirst().getValue().toString().contains(hostName));
    assertTrue(cert.getIssuerDN().toString().contains(scmId));
    assertTrue(cert.getIssuerDN().toString().contains(clusterId));

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

  private static X509CertificateHolder generateX509CertHolder(
      SecurityConfig conf, KeyPair keyPair, LocalDateTime startDate,
      Duration certLifetime) throws Exception {
    if (keyPair == null) {
      keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    }
    LocalDateTime start = startDate == null ? LocalDateTime.now() : startDate;
    LocalDateTime end = start.plus(certLifetime);
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

  private static X509CertificateHolder generateX509CertHolder(
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
    LocalDateTime start = LocalDateTime.now();
    Duration certDuration = conf.getDefaultCertDuration();
    X509CertificateHolder certificateHolder =
        approver.sign(conf, rootKeyPair.getPrivate(),
            new X509CertificateHolder(rootCert.getEncoded()),
            Date.from(start.atZone(ZoneId.systemDefault()).toInstant()),
            Date.from(start.plus(certDuration)
                .atZone(ZoneId.systemDefault()).toInstant()),
            csrBuilder.build(), "test", clusterId,
            String.valueOf(System.nanoTime()));
    return certificateHolder;
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

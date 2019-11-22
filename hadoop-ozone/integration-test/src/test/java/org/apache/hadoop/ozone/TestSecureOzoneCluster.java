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
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.test.LambdaTestUtils;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.jcajce.JcaX509CertificateHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.net.ServerSocketUtil.getPort;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_AUTH_METHOD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_EXPIRED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.slf4j.event.Level.INFO;

/**
 * Test class to for security enabled Ozone cluster.
 */
@InterfaceAudience.Private
public final class TestSecureOzoneCluster {

  private static final String TEST_USER = "testUgiUser@EXAMPLE.COM";
  private static final String COMPONENT = "test";
  private static final int CLIENT_TIMEOUT = 2_000;
  private static final String OM_CERT_SERIAL_ID = "9879877970576";
  private static final Logger LOG = LoggerFactory
      .getLogger(TestSecureOzoneCluster.class);

  @Rule
  public Timeout timeout = new Timeout(80_000);

  @Rule
  public TemporaryFolder folder= new TemporaryFolder();

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
  private String host;
  private String clusterId;
  private String scmId;
  private String omId;
  private OzoneManagerProtocolClientSideTranslatorPB omClient;

  @Before
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

      DefaultMetricsSystem.setMiniClusterMode(true);
      final String path = folder.newFolder().toString();
      Path metaDirPath = Paths.get(path, "om-meta");
      conf.set(OZONE_METADATA_DIRS, metaDirPath.toString());
      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
      conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());

      workDir = GenericTestUtils.getTestDir(getClass().getSimpleName());

      startMiniKdc();
      setSecureConfig();
      createCredentialsInKDC();
      generateKeyPair();
//      OzoneManager.setTestSecureOmFlag(true);
    } catch (Exception e) {
      LOG.error("Failed to initialize TestSecureOzoneCluster", e);
    }
  }

  @After
  public void stop() {
    try {
      stopMiniKdc();
      if (scm != null) {
        scm.stop();
      }
      IOUtils.closeQuietly(om);
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
  public void testSecureScmStartupSuccess() throws Exception {

    initSCM();
    scm = StorageContainerManager.createSCM(conf);
    //Reads the SCM Info from SCM instance
    ScmInfo scmInfo = scm.getClientProtocolServer().getScmInfo();
    assertEquals(clusterId, scmInfo.getClusterId());
    assertEquals(scmId, scmInfo.getScmId());
  }

  @Test
  public void testSCMSecurityProtocol() throws Exception {

    initSCM();
    scm = HddsTestUtils.getScm(conf);
    //Reads the SCM Info from SCM instance
    try {
      scm.start();

      // Case 1: User with Kerberos credentials should succeed.
      UserGroupInformation ugi =
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(
              testUserPrincipal, testUserKeytab.getCanonicalPath());
      ugi.setAuthenticationMethod(KERBEROS);
      SCMSecurityProtocol scmSecurityProtocolClient =
          HddsClientUtils.getScmSecurityClient(conf, ugi);
      assertNotNull(scmSecurityProtocolClient);
      String caCert = scmSecurityProtocolClient.getCACertificate();
      assertNotNull(caCert);
      LambdaTestUtils.intercept(RemoteException.class, "Certificate not found",
          () -> scmSecurityProtocolClient.getCertificate("1"));

      // Case 2: User without Kerberos credentials should fail.
      ugi = UserGroupInformation.createRemoteUser("test");
      ugi.setAuthenticationMethod(AuthMethod.TOKEN);
      SCMSecurityProtocol finalScmSecurityProtocolClient =
          HddsClientUtils.getScmSecurityClient(conf, ugi);

      String cannotAuthMessage = "Client cannot authenticate via:[KERBEROS]";
      LambdaTestUtils.intercept(IOException.class, cannotAuthMessage,
          finalScmSecurityProtocolClient::getCACertificate);
      LambdaTestUtils.intercept(IOException.class, cannotAuthMessage,
          () -> finalScmSecurityProtocolClient.getCertificate("1"));
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  private void initSCM() throws IOException {
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();

    final String path = folder.newFolder().toString();
    Path scmPath = Paths.get(path, "scm-meta");
    Files.createDirectories(scmPath);
    conf.set(OZONE_METADATA_DIRS, scmPath.toString());
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    scmStore.setClusterId(clusterId);
    scmStore.setScmId(scmId);
    // writes the version file properties
    scmStore.initialize();
  }

  @Test
  public void testSecureScmStartupFailure() throws Exception {
    initSCM();
    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, "");
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");

    LambdaTestUtils.intercept(IOException.class,
        "Running in secure mode, but config doesn't have a keytab",
        () -> StorageContainerManager.createSCM(conf));

    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY,
        "scm/_HOST@EXAMPLE.com");
    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        "/etc/security/keytabs/scm.keytab");

    testCommonKerberosFailures(
        () -> StorageContainerManager.createSCM(conf));

  }

  private void testCommonKerberosFailures(Callable<?> test) throws Exception {
    LambdaTestUtils.intercept(KerberosAuthException.class,
        "failure to login: for principal:",
        test);

    String invalidValue = "OAuth2";
    conf.set(HADOOP_SECURITY_AUTHENTICATION, invalidValue);
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid attribute value for " + HADOOP_SECURITY_AUTHENTICATION
            + " of " + invalidValue,
        test);

    conf.set(HADOOP_SECURITY_AUTHENTICATION, "KERBEROS_SSL");
    LambdaTestUtils.intercept(AuthenticationException.class,
        "KERBEROS_SSL authentication method not",
        test);
  }

  /**
   * Tests the secure om Initialization Failure.
   */
  @Test
  public void testSecureOMInitializationFailure() throws Exception {
    initSCM();
    // Create a secure SCM instance as om client will connect to it
    scm = StorageContainerManager.createSCM(conf);
    setupOm(conf);
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "non-existent-user@EXAMPLE.com");
    testCommonKerberosFailures(() -> OzoneManager.createOm(conf));
  }

  /**
   * Tests the secure om Initialization success.
   */
  @Test
  public void testSecureOmInitializationSuccess() throws Exception {
    initSCM();
    // Create a secure SCM instance as om client will connect to it
    scm = StorageContainerManager.createSCM(conf);
    LogCapturer logs = LogCapturer.captureLogs(OzoneManager.getLogger());
    GenericTestUtils.setLogLevel(OzoneManager.getLogger(), INFO);

    setupOm(conf);
    try {
      om.start();
    } catch (Exception ex) {
      // Expects timeout failure from scmClient in om but om user login via
      // kerberos should succeed.
      assertTrue(logs.getOutput().contains("Ozone Manager login successful"));
    }
  }

  /**
   * Performs following tests for delegation token.
   * 1. Get valid delegation token
   * 2. Test successful token renewal.
   * 3. Client can authenticate using token.
   * 4. Delegation token renewal without Kerberos auth fails.
   * 5. Test success of token cancellation.
   * 5. Test failure of token cancellation.
   */
  @Test
  public void testDelegationToken() throws Exception {

    // Capture logs for assertions
    LogCapturer logs = LogCapturer.captureLogs(Server.AUDITLOG);
    LogCapturer omLogs = LogCapturer.captureLogs(OzoneManager.getLogger());
    GenericTestUtils
        .setLogLevel(LoggerFactory.getLogger(Server.class.getName()), INFO);

    // Setup secure OM for start
    setupOm(conf);
    long omVersion =
        RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    try {
      // Start OM
      om.setCertClient(new CertificateClientTestImpl(conf));
      om.start();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String username = ugi.getUserName();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
              OmUtils.getOmAddress(conf), ugi, conf,
              NetUtils.getDefaultSocketFactory(conf),
              CLIENT_TIMEOUT), RandomStringUtils.randomAscii(5));

      // Assert if auth was successful via Kerberos
      assertFalse(logs.getOutput().contains(
          "Auth successful for " + username + " (auth:KERBEROS)"));

      // Case 1: Test successful delegation token.
      Token<OzoneTokenIdentifier> token = omClient
          .getDelegationToken(new Text("om"));

      // Case 2: Test successful token renewal.
      long renewalTime = omClient.renewDelegationToken(token);
      assertTrue(renewalTime > 0);

      // Check if token is of right kind and renewer is running om instance
      assertNotNull(token);
      assertEquals("OzoneToken", token.getKind().toString());
      assertEquals(OmUtils.getOmRpcAddress(conf),
          token.getService().toString());
      omClient.close();

      // Create a remote ugi and set its authentication method to Token
      UserGroupInformation testUser = UserGroupInformation
          .createRemoteUser(TEST_USER);
      testUser.addToken(token);
      testUser.setAuthenticationMethod(AuthMethod.TOKEN);
      UserGroupInformation.setLoginUser(testUser);

      // Get Om client, this time authentication should happen via Token
      testUser.doAs((PrivilegedExceptionAction<Void>) () -> {
        omClient = new OzoneManagerProtocolClientSideTranslatorPB(
            RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
                OmUtils.getOmAddress(conf), testUser, conf,
                NetUtils.getDefaultSocketFactory(conf), CLIENT_TIMEOUT),
            RandomStringUtils.randomAscii(5));
        return null;
      });

      // Case 3: Test Client can authenticate using token.
      assertFalse(logs.getOutput().contains(
          "Auth successful for " + username + " (auth:TOKEN)"));
      OzoneTestUtils.expectOmException(VOLUME_NOT_FOUND,
          () -> omClient.deleteVolume("vol1"));
      assertTrue(logs.getOutput().contains("Auth successful for "
          + username + " (auth:TOKEN)"));

      // Case 4: Test failure of token renewal.
      // Call to renewDelegationToken will fail but it will confirm that
      // initial connection via DT succeeded
      omLogs.clearOutput();

      OMException ex = LambdaTestUtils.intercept(OMException.class,
          "INVALID_AUTH_METHOD",
          () -> omClient.renewDelegationToken(token));
      assertEquals(INVALID_AUTH_METHOD, ex.getResult());
      assertTrue(logs.getOutput().contains(
          "Auth successful for " + username + " (auth:TOKEN)"));
      omLogs.clearOutput();
      //testUser.setAuthenticationMethod(AuthMethod.KERBEROS);
      UserGroupInformation.setLoginUser(ugi);
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
              OmUtils.getOmAddress(conf), ugi, conf,
              NetUtils.getDefaultSocketFactory(conf),
              Client.getRpcTimeout(conf)), RandomStringUtils.randomAscii(5));

      // Case 5: Test success of token cancellation.
      omClient.cancelDelegationToken(token);
      omClient.close();

      // Wait for client to timeout
      Thread.sleep(CLIENT_TIMEOUT);

      assertFalse(logs.getOutput().contains("Auth failed for"));

      // Case 6: Test failure of token cancellation.
      // Get Om client, this time authentication using Token will fail as
      // token is not in cache anymore.
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
              OmUtils.getOmAddress(conf), testUser, conf,
              NetUtils.getDefaultSocketFactory(conf),
              Client.getRpcTimeout(conf)), RandomStringUtils.randomAscii(5));
      ex = LambdaTestUtils.intercept(OMException.class,
          "Cancel delegation token failed",
          () -> omClient.cancelDelegationToken(token));
      assertEquals(TOKEN_ERROR_OTHER, ex.getResult());
      assertTrue(logs.getOutput().contains("Auth failed for"));
    } finally {
      om.stop();
      om.join();
    }
  }

  private void generateKeyPair() throws Exception {
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(conf);
    KeyPair keyPair = keyGenerator.generateKey();
    KeyCodec pemWriter = new KeyCodec(new SecurityConfig(conf), COMPONENT);
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

    // Setup secure OM for start.
    OzoneConfiguration newConf = new OzoneConfiguration(conf);
    int tokenMaxLifetime = 500;
    newConf.setLong(DELEGATION_TOKEN_MAX_LIFETIME_KEY, tokenMaxLifetime);
    setupOm(newConf);
    long omVersion =
        RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    OzoneManager.setTestSecureOmFlag(true);
    // Start OM

    try {
      om.setCertClient(new CertificateClientTestImpl(conf));
      om.start();

      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(RPC.getProxy(
          OzoneManagerProtocolPB.class, omVersion, OmUtils.getOmAddress(conf),
          ugi, conf, NetUtils.getDefaultSocketFactory(conf),
          CLIENT_TIMEOUT), RandomStringUtils.randomAscii(5));

      // Since client is already connected get a delegation token
      Token<OzoneTokenIdentifier> token = omClient.getDelegationToken(
          new Text("om"));

      // Check if token is of right kind and renewer is running om instance
      assertNotNull(token);
      assertEquals("OzoneToken", token.getKind().toString());
      assertEquals(OmUtils.getOmRpcAddress(conf),
          token.getService().toString());

      // Renew delegation token
      long expiryTime = omClient.renewDelegationToken(token);
      assertTrue(expiryTime > 0);
      omLogs.clearOutput();

      // Test failure of delegation renewal
      // 1. When token maxExpiryTime exceeds
      Thread.sleep(tokenMaxLifetime);
      OMException ex = LambdaTestUtils.intercept(OMException.class,
          "TOKEN_EXPIRED",
          () -> omClient.renewDelegationToken(token));
      assertEquals(TOKEN_EXPIRED, ex.getResult());
      omLogs.clearOutput();

      // 2. When renewer doesn't match (implicitly covers when renewer is
      // null or empty )
      Token<OzoneTokenIdentifier> token2 = omClient.getDelegationToken(
          new Text("randomService"));
      assertNotNull(token2);
      LambdaTestUtils.intercept(OMException.class,
          "Delegation token renewal failed",
          () -> omClient.renewDelegationToken(token2));
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
      LambdaTestUtils.intercept(OMException.class,
          "Delegation token renewal failed",
          () -> omClient.renewDelegationToken(tamperedToken));
      assertTrue(omLogs.getOutput().contains("can't be found in " +
          "cache"));
      omLogs.clearOutput();

    } finally {
      om.stop();
      om.join();
    }
  }

  private void setupOm(OzoneConfiguration config) throws Exception {
    OMStorage omStore = new OMStorage(config);
    omStore.setClusterId("testClusterId");
    omStore.setScmId("testScmId");
    omStore.setOmCertSerialId(OM_CERT_SERIAL_ID);
    // writes the version file properties
    omStore.initialize();
    OzoneManager.setTestSecureOmFlag(true);
    om = OzoneManager.createOm(config);
  }

  @Test
  public void testGetS3Secret() throws Exception {

    // Setup secure OM for start
    setupOm(conf);
    long omVersion =
        RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    try {
      // Start OM
      om.setCertClient(new CertificateClientTestImpl(conf));
      om.start();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String username = ugi.getUserName();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
              OmUtils.getOmAddress(conf), ugi, conf,
              NetUtils.getDefaultSocketFactory(conf),
              CLIENT_TIMEOUT), RandomStringUtils.randomAscii(5));

      //Creates a secret since it does not exist
      S3SecretValue attempt1 = omClient.getS3Secret(username);

      //Fetches the secret from db since it was created in previous step
      S3SecretValue attempt2 = omClient.getS3Secret(username);

      //secret fetched on both attempts must be same
      assertEquals(attempt1.getAwsSecret(), attempt2.getAwsSecret());

      //access key fetched on both attempts must be same
      assertEquals(attempt1.getAwsAccessKey(), attempt2.getAwsAccessKey());


      try {
        omClient.getS3Secret("HADOOP/JOHNDOE");
        fail("testGetS3Secret failed");
      } catch (IOException ex) {
        GenericTestUtils.assertExceptionContains("USER_MISMATCH", ex);
      }
    } finally {
      IOUtils.closeQuietly(om);
    }
  }

  /**
   * Tests functionality to init secure OM when it is already initialized.
   */
  @Test
  public void testSecureOmReInit() throws Exception {
    LogCapturer omLogs =
        LogCapturer.captureLogs(OzoneManager.getLogger());
    omLogs.clearOutput();

    /*
     * As all these processes run inside the same JVM, there are issues around
     * the Hadoop UGI if different processes run with different principals.
     * In this test, the OM has to contact the SCM to download certs. SCM runs
     * as scm/host@REALM, but the OM logs in as om/host@REALM, and then the test
     * fails, and the OM is unable to contact the SCM due to kerberos login
     * issues. To work around that, have the OM run as the same principal as the
     * SCM, and then the test passes.
     *
     * TODO: Need to look into this further to see if there is a better way to
     *       address this problem.
     */
    String realm = miniKdc.getRealm();
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "scm/" + host + "@" + realm);
    omKeyTab = new File(workDir, "scm.keytab");
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        omKeyTab.getAbsolutePath());

    initSCM();
    try {
      scm = HddsTestUtils.getScm(conf);
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
      OzoneManager.omInit(conf);
      om.stop();
      om = OzoneManager.createOm(conf);

      assertNotNull(om.getCertificateClient());
      assertNotNull(om.getCertificateClient().getPublicKey());
      assertNotNull(om.getCertificateClient().getPrivateKey());
      assertNotNull(om.getCertificateClient().getCertificate());
      assertTrue(omLogs.getOutput().contains("Init response: GETCERT"));
      assertTrue(omLogs.getOutput().contains("Successfully stored " +
          "SCM signed certificate"));
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
        LogCapturer.captureLogs(OzoneManager.getLogger());
    omLogs.clearOutput();
    initSCM();
    try {
      scm = HddsTestUtils.getScm(conf);
      scm.start();

      OMStorage omStore = new OMStorage(conf);
      initializeOmStorage(omStore);
      OzoneManager.setTestSecureOmFlag(true);
      om = OzoneManager.createOm(conf);

      assertNotNull(om.getCertificateClient());
      assertNotNull(om.getCertificateClient().getPublicKey());
      assertNotNull(om.getCertificateClient().getPrivateKey());
      assertNotNull(om.getCertificateClient().getCertificate());
      assertTrue(omLogs.getOutput().contains("Init response: GETCERT"));
      assertTrue(omLogs.getOutput().contains("Successfully stored " +
          "SCM signed certificate"));
      X509Certificate certificate = om.getCertificateClient().getCertificate();
      validateCertificate(certificate);
      String pemEncodedCACert =
          scm.getSecurityProtocolServer().getCACertificate();
      X509Certificate caCert = CertificateCodec.getX509Cert(pemEncodedCACert);
      X509Certificate caCertStored = om.getCertificateClient()
          .getCertificate(caCert.getSerialNumber().toString());
      assertEquals(caCert, caCertStored);
    } finally {
      if (scm != null) {
        scm.stop();
      }
      IOUtils.closeQuietly(om);
    }

  }

  public void validateCertificate(X509Certificate cert) throws Exception {

    // Assert that we indeed have a self signed certificate.
    X500Name x500Issuer = new JcaX509CertificateHolder(cert).getIssuer();
    RDN cn = x500Issuer.getRDNs(BCStyle.CN)[0];
    String hostName = InetAddress.getLocalHost().getHostName();
    String scmUser = "scm@" + hostName;
    assertEquals(scmUser, cn.getFirst().getValue().toString());

    // Subject name should be om login user in real world but in this test
    // UGI has scm user context.
    assertEquals(scmUser, cn.getFirst().getValue().toString());

    LocalDate today = LocalDateTime.now().toLocalDate();
    Date invalidDate;

    // Make sure the end date is honored.
    invalidDate = java.sql.Date.valueOf(today.plus(1, ChronoUnit.DAYS));
    assertTrue(cert.getNotAfter().after(invalidDate));

    invalidDate = java.sql.Date.valueOf(today.plus(400, ChronoUnit.DAYS));
    assertTrue(cert.getNotAfter().before(invalidDate));

    assertTrue(cert.getSubjectDN().toString().contains(scmId));
    assertTrue(cert.getSubjectDN().toString().contains(clusterId));

    assertTrue(cert.getIssuerDN().toString().contains(scmUser));
    assertTrue(cert.getIssuerDN().toString().contains(scmId));
    assertTrue(cert.getIssuerDN().toString().contains(clusterId));

    // Verify that certificate matches the public key.
    String encodedKey1 = cert.getPublicKey().toString();
    String encodedKey2 = om.getCertificateClient().getPublicKey().toString();
    assertEquals(encodedKey1, encodedKey2);
  }

  private void initializeOmStorage(OMStorage omStorage) throws IOException {
    if (omStorage.getState() == Storage.StorageState.INITIALIZED) {
      return;
    }
    omStorage.setClusterId(clusterId);
    omStorage.setScmId(scmId);
    omStorage.setOmId(omId);
    // Initialize ozone certificate client if security is enabled.
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      OzoneManager.initializeSecurity(conf, omStorage);
    }
    omStorage.initialize();
  }
}

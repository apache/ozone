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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_AUTH_METHOD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.slf4j.event.Level.INFO;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.client.ScmTopologyClient;
import org.apache.hadoop.hdds.scm.ha.HASecurityUtils;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmBlockLocationTestingClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to for security enabled Ozone cluster.
 */
public final class TestDelegationToken {

  private static final String TEST_USER = "testUgiUser@EXAMPLE.COM";
  private static final String COMPONENT = "test";
  private static final int CLIENT_TIMEOUT = 2_000;
  private static final String OM_CERT_SERIAL_ID = "9879877970576";
  private static final Logger LOG = LoggerFactory
      .getLogger(TestDelegationToken.class);

  @TempDir
  private Path folder;
  @TempDir
  private File workDir;

  private MiniKdc miniKdc;
  private OzoneConfiguration conf;
  private File scmKeytab;
  private File spnegoKeytab;
  private File omKeyTab;
  private File testUserKeytab;
  private String testUserPrincipal;
  private StorageContainerManager scm;
  private OzoneManager om;
  private String host;
  private String clusterId = UUID.randomUUID().toString();
  private String scmId = UUID.randomUUID().toString();
  private OzoneManagerProtocolClientSideTranslatorPB omClient;

  public static Stream<Boolean> options() {
    return Stream.of(false, true);
  }

  @BeforeAll
  public static void setup() {
    ExitUtils.disableSystemExit();
  }

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

      DefaultMetricsSystem.setMiniClusterMode(true);
      final String path = folder.resolve("om-meta").toString();
      Path metaDirPath = Paths.get(path, "om-meta");
      conf.set(OZONE_METADATA_DIRS, metaDirPath.toString());
      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
      conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());

      startMiniKdc();
      setSecureConfig();
      createCredentialsInKDC();
      generateKeyPair();
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

  private void initSCM() throws IOException {
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    scmStore.setClusterId(clusterId);
    scmStore.setScmId(scmId);
    HASecurityUtils.initializeSecurity(scmStore, conf,
        InetAddress.getLocalHost().getHostName(), true);
    scmStore.setPrimaryScmNodeId(scmId);
    // writes the version file properties
    scmStore.initialize();
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
  @ParameterizedTest
  @MethodSource("options")
  public void testDelegationToken(boolean useIp) throws Exception {
    initSCM();
    scm = HddsTestUtils.getScmSimple(conf);
    scm.start();

    // Capture logs for assertions
    LogCapturer logs = LogCapturer.captureLogs(Server.AUDITLOG);
    LogCapturer omLogs = LogCapturer.captureLogs(OzoneManager.class);
    GenericTestUtils.setLogLevel(Server.class, INFO);
    SecurityUtil.setTokenServiceUseIp(useIp);

    // Setup secure OM for start
    setupOm(conf);

    //These are two very important lines: ProtobufRpcEngine uses ClientCache
    //which caches clients until no more references. Cache key is the
    //SocketFactory which means that we use one Client instance for
    //all the Hadoop RPC.
    //
    //Hadoop Client caches connections with a connection pool. Even if you
    //close the client here, if you have ANY Hadoop RPC clients which uses the
    //same Client, connections can be reused.
    //
    //Here we closed all the OTHER Hadoop RPC clients to have only the clients
    //from this unit test.
    //
    //With this approach all the following client.close() calls trigger a
    //Client.close (if there is no other open Hadoop RPC client) which triggers
    //connection close for all the available connection.
    //
    //The following test tests the authorization of the new client calls, it
    //requires a real connection close and connection open as the authorization
    //is part the initial handhsake of Hadoop RPC.
    om.getScmClient().getBlockClient().close();
    om.getScmClient().getContainerClient().close();

    try {
      // Start OM
      om.setCertClient(new CertificateClientTestImpl(conf));
      om.setScmTopologyClient(new ScmTopologyClient(
          new ScmBlockLocationTestingClient(null, null, 0)));
      om.start();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String username = ugi.getUserName();
      logs.clearOutput();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(conf, ugi, null),
          RandomStringUtils.secure().nextAscii(5));

      // Assert if auth was successful via Kerberos
      assertThat(logs.getOutput()).doesNotContain(
          "Auth successful for " + username + " (auth:KERBEROS)");

      // Case 1: Test successful delegation token.
      Token<OzoneTokenIdentifier> token = omClient
          .getDelegationToken(new Text("scm"));

      // Case 2: Test successful token renewal.
      long renewalTime = omClient.renewDelegationToken(token);
      assertThat(renewalTime).isGreaterThan(0);

      // Check if token is of right kind and renewer is running om instance
      assertNotNull(token);
      assertEquals("OzoneToken", token.getKind().toString());
      assertEquals(SecurityUtil.buildTokenService(
          om.getNodeDetails().getRpcAddress()).toString(),
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
            OmTransportFactory.create(conf, testUser, null),
            RandomStringUtils.secure().nextAscii(5));
        return null;
      });

      // Case 3: Test Client can authenticate using token.
      assertThat(logs.getOutput()).doesNotContain(
          "Auth successful for " + username + " (auth:TOKEN)");
      OzoneTestUtils.expectOmException(VOLUME_NOT_FOUND,
          () -> omClient.deleteVolume("vol1"));
      assertThat(logs.getOutput())
          .contains("Auth successful for " + username + " (auth:TOKEN)");

      // Case 4: Test failure of token renewal.
      // Call to renewDelegationToken will fail but it will confirm that
      // initial connection via DT succeeded
      omLogs.clearOutput();

      OMException ex = assertThrows(OMException.class,
          () -> omClient.renewDelegationToken(token));
      assertEquals(INVALID_AUTH_METHOD, ex.getResult());
      assertThat(logs.getOutput()).contains(
          "Auth successful for " + username + " (auth:TOKEN)");
      omLogs.clearOutput();
      //testUser.setAuthenticationMethod(AuthMethod.KERBEROS);
      omClient.close();
      UserGroupInformation.setLoginUser(ugi);
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(conf, ugi, null),
          RandomStringUtils.secure().nextAscii(5));

      // Case 5: Test success of token cancellation.
      omClient.cancelDelegationToken(token);
      omClient.close();

      // Wait for client to timeout
      Thread.sleep(CLIENT_TIMEOUT);

      assertThat(logs.getOutput()).doesNotContain("Auth failed for");

      // Case 6: Test failure of token cancellation.
      // Get Om client, this time authentication using Token will fail as
      // token is not in cache anymore.
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(conf, testUser, null),
          RandomStringUtils.secure().nextAscii(5));
      ex = assertThrows(OMException.class,
          () -> omClient.cancelDelegationToken(token));
      assertEquals(TOKEN_ERROR_OTHER, ex.getResult());
      assertThat(ex.getMessage()).contains("Cancel delegation token failed");
      assertThat(logs.getOutput()).contains("Auth failed for");
    } finally {
      om.stop();
      om.join();
    }
  }

  private void generateKeyPair() throws Exception {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = keyGenerator.generateKey();
    KeyStorage keyStorage = new KeyStorage(securityConfig, COMPONENT);
    keyStorage.storeKeyPair(keyPair);
  }

  private void setupOm(OzoneConfiguration config) throws Exception {
    OMStorage omStore = new OMStorage(config);
    omStore.setClusterId(clusterId);
    omStore.setOmCertSerialId(OM_CERT_SERIAL_ID);
    // writes the version file properties
    omStore.initialize();

    // OM uses scm/host@EXAMPLE.COM to access SCM
    config.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "scm/" + host + "@" + miniKdc.getRealm());
    omKeyTab = new File(workDir, "scm.keytab");
    config.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, omKeyTab.getAbsolutePath());

    OzoneManager.setTestSecureOmFlag(true);
    om = OzoneManager.createOm(config);
  }
}

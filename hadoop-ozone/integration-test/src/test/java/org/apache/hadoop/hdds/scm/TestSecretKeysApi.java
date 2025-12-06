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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_EXPIRY_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_CHECK_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_DURATION;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getSecretKeyClientForDatanode;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.DELEGATION_REMOVER_SCAN_INTERVAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocol;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.ExitUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test to verify symmetric SecretKeys APIs in a secure cluster.
 */

@InterfaceAudience.Private
public final class TestSecretKeysApi {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestSecretKeysApi.class);
  private MiniKdc miniKdc;
  private OzoneConfiguration conf;
  @TempDir
  private File workDir;
  private File ozoneKeytab;
  private File spnegoKeytab;
  private File testUserKeytab;
  private String testUserPrincipal;
  private String ozonePrincipal;
  private MiniOzoneHAClusterImpl cluster;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");

    ExitUtils.disableSystemExit();
    ExitUtil.disableSystemExit();

    startMiniKdc();
    setSecureConfig();
    createCredentialsInKDC();
  }

  @AfterEach
  public void stop() {
    miniKdc.stop();
    IOUtils.closeQuietly(cluster);
  }

  private void createCredentialsInKDC() throws Exception {
    SCMHTTPServerConfig httpServerConfig =
        conf.getObject(SCMHTTPServerConfig.class);
    createPrincipal(ozoneKeytab, ozonePrincipal);
    createPrincipal(spnegoKeytab, httpServerConfig.getKerberosPrincipal());
    createPrincipal(testUserKeytab, testUserPrincipal);
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

  private void setSecureConfig() throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    String host = InetAddress.getLocalHost().getCanonicalHostName()
        .toLowerCase();

    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());

    String curUser = UserGroupInformation.getCurrentUser().getUserName();
    conf.set(OZONE_ADMINISTRATORS, curUser);

    String realm = miniKdc.getRealm();
    String hostAndRealm = host + "@" + realm;
    ozonePrincipal = "scm/" + hostAndRealm;
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
    conf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_SCM/" + hostAndRealm);
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
    conf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_OM/" + hostAndRealm);
    conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);

    ozoneKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");
    testUserKeytab = new File(workDir, "testuser.keytab");
    testUserPrincipal = "test@" + realm;

    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        ozoneKeytab.getAbsolutePath());
    conf.set(HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        ozoneKeytab.getAbsolutePath());
    conf.set(OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE,
        spnegoKeytab.getAbsolutePath());
    conf.set(HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY,
        ozoneKeytab.getAbsolutePath());

    conf.setBoolean(HADOOP_SECURITY_AUTHORIZATION, true);
  }

  /**
   * Test secret key apis in happy case.
   */
  @Test
  @Flaky("HDDS-8900")
  public void testSecretKeyApiSuccess() throws Exception {
    enableBlockToken();
    // set a low rotation period, of 1s, expiry is 3s, expect 3 active keys
    // at any moment.
    conf.set(HDDS_SECRET_KEY_ROTATE_CHECK_DURATION, "100ms");
    conf.set(HDDS_SECRET_KEY_ROTATE_DURATION, "1s");
    conf.set(HDDS_SECRET_KEY_EXPIRY_DURATION, "3000ms");
    conf.set(DELEGATION_TOKEN_MAX_LIFETIME_KEY, "1500ms");
    conf.set(DELEGATION_REMOVER_SCAN_INTERVAL_KEY, "100ms");

    startCluster(3);
    SecretKeyProtocol secretKeyProtocol = getSecretKeyProtocol();

    // start the test when keys are full.
    GenericTestUtils.waitFor(() -> {
      try {
        return secretKeyProtocol.getAllSecretKeys().size() >= 3;
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }, 100, 4_000);

    ManagedSecretKey initialKey = secretKeyProtocol.getCurrentSecretKey();
    assertNotNull(initialKey);
    List<ManagedSecretKey> initialKeys = secretKeyProtocol.getAllSecretKeys();
    assertEquals(initialKey, initialKeys.get(0));

    LOG.info("Initial active key: {}", initialKey);
    LOG.info("Initial keys: {}", initialKeys);

    // wait for the next rotation.
    GenericTestUtils.waitFor(() -> {
      try {
        ManagedSecretKey newCurrentKey =
            secretKeyProtocol.getCurrentSecretKey();
        return !newCurrentKey.equals(initialKey);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }, 100, 1500);
    ManagedSecretKey  updatedKey = secretKeyProtocol.getCurrentSecretKey();
    List<ManagedSecretKey>  updatedKeys = secretKeyProtocol.getAllSecretKeys();

    LOG.info("Updated active key: {}", updatedKey);
    LOG.info("Updated keys: {}", updatedKeys);

    assertEquals(updatedKey, updatedKeys.get(0));
    assertEquals(initialKey, updatedKeys.get(1));

    // assert getSecretKey by ID.
    ManagedSecretKey keyById = secretKeyProtocol.getSecretKey(
        updatedKey.getId());
    assertNotNull(keyById);
    ManagedSecretKey nonExisting = secretKeyProtocol.getSecretKey(
        UUID.randomUUID());
    assertNull(nonExisting);

    testSecretKeyAuthorization();
  }

  /**
   * Verify API behavior.
   */
  @Test
  public void testSecretKeyApi() throws Exception {
    startCluster(1);
    SecretKeyProtocol secretKeyProtocol = getSecretKeyProtocol();
    assertNull(secretKeyProtocol.getSecretKey(UUID.randomUUID()));
    assertEquals(1, secretKeyProtocol.getAllSecretKeys().size());
  }

  /**
   * Verify API behavior when SCM leader fails.
   */
  @Test
  public void testSecretKeyAfterSCMFailover() throws Exception {
    enableBlockToken();
    // set a long duration period, so that no rotation happens during SCM
    // leader change.
    conf.set(HDDS_SECRET_KEY_ROTATE_CHECK_DURATION, "10m");
    conf.set(HDDS_SECRET_KEY_ROTATE_DURATION, "1d");
    conf.set(HDDS_SECRET_KEY_EXPIRY_DURATION, "7d");
    conf.set(DELEGATION_TOKEN_MAX_LIFETIME_KEY, "5d");

    startCluster(3);
    SecretKeyProtocol securityProtocol = getSecretKeyProtocol();
    List<ManagedSecretKey> keysInitial = securityProtocol.getAllSecretKeys();
    LOG.info("Keys before fail over: {}.", keysInitial);

    // turn the current SCM leader off.
    StorageContainerManager activeSCM = cluster.getActiveSCM();
    cluster.shutdownStorageContainerManager(activeSCM);
    // wait for
    cluster.waitForSCMToBeReady();

    List<ManagedSecretKey> keysAfter = securityProtocol.getAllSecretKeys();
    LOG.info("Keys after fail over: {}.", keysAfter);

    assertEquals(keysInitial.size(), keysAfter.size());
    for (int i = 0; i < keysInitial.size(); i++) {
      assertEquals(keysInitial.get(i), keysAfter.get(i));
    }
  }

  private void testSecretKeyAuthorization() throws Exception {
    // When HADOOP_SECURITY_AUTHORIZATION is enabled, SecretKey protocol
    // is only available for Datanode and OM, any other authenticated user
    // can't access the protocol.
    SecretKeyProtocol secretKeyProtocol =
        getSecretKeyProtocol(testUserPrincipal, testUserKeytab);
    RemoteException ex =
        assertThrows(RemoteException.class,
            secretKeyProtocol::getCurrentSecretKey);
    assertEquals(AuthorizationException.class.getName(), ex.getClassName());
    assertThat(ex.getMessage()).contains(
        "User test@EXAMPLE.COM (auth:KERBEROS) is not authorized " +
            "for protocol");
  }

  @Test
  public void testSecretKeyWithoutAuthorization() throws Exception {
    enableBlockToken();
    conf.setBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
    startCluster(1);

    // When HADOOP_SECURITY_AUTHORIZATION is not enabled, any other
    // authenticated user can access the protocol.
    SecretKeyProtocol secretKeyProtocol =
        getSecretKeyProtocol(testUserPrincipal, testUserKeytab);
    assertNotNull(secretKeyProtocol.getCurrentSecretKey());
  }

  private void startCluster(int numSCMs)
      throws IOException, TimeoutException, InterruptedException {
    OzoneManager.setTestSecureOmFlag(true);
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId("TestSecretKey")
        .setNumOfStorageContainerManagers(numSCMs)
        .setNumOfOzoneManagers(1);

    cluster = builder.build();
    cluster.waitForClusterToBeReady();
  }

  @Nonnull
  private SecretKeyProtocol getSecretKeyProtocol() throws IOException {
    return getSecretKeyProtocol(ozonePrincipal, ozoneKeytab);
  }

  @Nonnull
  private SecretKeyProtocol getSecretKeyProtocol(
      String user, File keyTab) throws IOException {
    UserGroupInformation ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
            user, keyTab.getCanonicalPath());
    ugi.setAuthenticationMethod(KERBEROS);
    return getSecretKeyClientForDatanode(conf, ugi);
  }

  private void enableBlockToken() {
    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
  }
}

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

import static java.time.Duration.between;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.security.symmetric.SecretKeyConfig.parseRotateDuration;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.google.common.collect.Maps;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test class to verify block token CLI commands functionality in a
 * secure cluster.
 */
@InterfaceAudience.Private
public final class TestBlockTokensCLI {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestBlockTokensCLI.class);

  @TempDir
  private static File workDir;
  private static MiniKdc miniKdc;
  private static OzoneAdmin ozoneAdmin;
  private static OzoneConfiguration conf;
  private static File ozoneKeytab;
  private static File spnegoKeytab;
  private static String omServiceId;
  private static String scmServiceId;
  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    ozoneAdmin = new OzoneAdmin();
    conf = ozoneAdmin.getOzoneConf();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");

    ExitUtils.disableSystemExit();

    omServiceId = "om-service-test";
    scmServiceId = "scm-service-test";

    startMiniKdc();
    setSecureConfig();
    createCredentialsInKDC();
    setSecretKeysConfig();
    startCluster();
    client = cluster.newClient();
  }

  @AfterAll
  public static void stop() {
    miniKdc.stop();
    IOUtils.close(LOG, client, cluster);
  }

  private SecretKeyManager getScmSecretKeyManager() {
    return cluster.getActiveSCM().getSecretKeyManager();
  }

  private static void setSecretKeysConfig() {
    // enable tokens
    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.setBoolean(HDDS_CONTAINER_TOKEN_ENABLED, true);
  }

  private static void createCredentialsInKDC() throws Exception {
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    SCMHTTPServerConfig httpServerConfig =
        conf.getObject(SCMHTTPServerConfig.class);
    createPrincipal(ozoneKeytab, scmConfig.getKerberosPrincipal());
    createPrincipal(spnegoKeytab, httpServerConfig.getKerberosPrincipal());
  }

  private static void createPrincipal(File keytab, String... principal)
      throws Exception {
    miniKdc.createPrincipal(keytab, principal);
  }

  private static void startMiniKdc() throws Exception {
    Properties securityProperties = MiniKdc.createConf();
    miniKdc = new MiniKdc(securityProperties, workDir);
    miniKdc.start();
  }

  private static void setSecureConfig() throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    String host = InetAddress.getLocalHost().getCanonicalHostName()
                      .toLowerCase();

    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());

    String curUser = UserGroupInformation.getCurrentUser().getUserName();
    conf.set(OZONE_ADMINISTRATORS, curUser);

    String realm = miniKdc.getRealm();
    String hostAndRealm = host + "@" + realm;
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);
    conf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_SCM/" + hostAndRealm);
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);
    conf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_OM/" + hostAndRealm);
    conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);

    ozoneKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");

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
  }

  @Test
  public void testFetchKeyOMAdminCommand() throws UnsupportedEncodingException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream, true, "UTF-8");
    System.setOut(printStream);

    String[] args =
        new String[]{"om", "fetch-key", "--service-id=" + omServiceId};
    ozoneAdmin.execute(args);

    String actualOutput = outputStream.toString("UTF-8");
    System.setOut(System.out);

    String actualUUID = testFetchKeyOMAdminCommandUtil(actualOutput);
    String expectedUUID =
        getScmSecretKeyManager().getCurrentSecretKey().getId().toString();
    assertEquals(expectedUUID, actualUUID);
  }

  private String testFetchKeyOMAdminCommandUtil(String output) {
    // Extract the current secret key id from the output
    String[] lines = output.split(System.lineSeparator());
    for (String line : lines) {
      if (line.startsWith("Current Secret Key ID: ")) {
        return line.substring("Current Secret Key ID: ".length()).trim();
      }
    }
    return null;
  }

  private boolean isForceFlagPresent(String[] args) {
    for (String arg : args) {
      if (arg.equals("--force")) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testRotateKeySCMAdminCommandWithForceFlag()
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> cluster.getScmLeader() != null, 100, 1000);
    InetSocketAddress address = cluster.getScmLeader().getClientRpcAddress();
    String hostPort = address.getHostName() + ":" + address.getPort();

    testRotateKeySCMAdminCommandUtil(createArgsForCommand(
        new String[]{"scm", "rotate", "--scm", hostPort, "--force"}));
  }

  @Test
  public void testRotateKeySCMAdminCommandWithoutForceFlag()
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> cluster.getScmLeader() != null, 100, 1000);
    InetSocketAddress address = cluster.getScmLeader().getClientRpcAddress();
    String hostPort = address.getHostName() + ":" + address.getPort();

    testRotateKeySCMAdminCommandUtil(
        createArgsForCommand(new String[]{"scm", "rotate", "--scm", hostPort}));
  }

  public void testRotateKeySCMAdminCommandUtil(String[] args) {
    // Get the initial secret key.
    String initialKey =
        getScmSecretKeyManager().getCurrentSecretKey().toString();

    // Assert that both initial key and subsequent key are the same before
    // rotating.
    String currentKey =
        getScmSecretKeyManager().getCurrentSecretKey().toString();
    assertEquals(initialKey, currentKey);

    // Rotate the secret key.
    ozoneAdmin.execute(args);

    // Get the new secret key.
    String newKey =
        getScmSecretKeyManager().getCurrentSecretKey().toString();

    // Assert that the old key and new key are not the same after rotating if
    // either:
    // 1. The rotation duration has surpassed, or
    // 2. The --force flag has been passed.
    // Otherwise, both keys should be the same.
    if (isForceFlagPresent(args) ||
        shouldRotate(getScmSecretKeyManager().getCurrentSecretKey())) {
      assertNotEquals(initialKey, newKey);
    } else {
      assertEquals(initialKey, newKey);
    }
  }

  private String getSetConfStringFromConf(String key) {
    return String.format("--set=%s=%s", key, conf.get(key));
  }

  public boolean shouldRotate(ManagedSecretKey currentKey) {
    Duration established = between(currentKey.getCreationTime(), Instant.now());
    return established.compareTo(parseRotateDuration(conf)) >= 0;
  }

  /**
   * Since ScmAdmin relies on ScmOption to generate configurations, it uses the
   * default configuration obtained from
   * {@link org.apache.hadoop.hdds.scm.cli.ScmOption#createScmClient(
   * OzoneConfiguration)} using createOzoneConfiguration().
   * In the absence of a better way to pass these configurations via integration
   * tests in the ozone admin shell, this method handles the passing of kerberos
   * and SCM HA configurations by creating strings in the "--set=key=value"
   * format.
   */
  private String[] createArgsForCommand(String[] additionalArgs) {
    OzoneConfiguration defaultConf = ozoneAdmin.getOzoneConf();
    Map<String, String> diff = Maps.difference(defaultConf.getOzoneProperties(),
        conf.getOzoneProperties()).entriesOnlyOnRight();
    String[] args = new String[diff.size() + additionalArgs.length];
    int i = 0;
    for (Map.Entry<String, String> entry : diff.entrySet()) {
      args[i++] = getSetConfStringFromConf(entry.getKey());
    }
    System.arraycopy(additionalArgs, 0, args, i, additionalArgs.length);
    return args;
  }

  private static void startCluster()
      throws IOException, TimeoutException, InterruptedException {
    OzoneManager.setTestSecureOmFlag(true);
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId(scmServiceId)
        .setOMServiceId(omServiceId)
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(3);

    cluster = builder.build();
    cluster.waitForClusterToBeReady();
  }
}

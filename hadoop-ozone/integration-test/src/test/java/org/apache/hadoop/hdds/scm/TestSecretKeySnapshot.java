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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.ha.SCMStateMachine;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test to verify that symmetric secret keys are correctly
 * synchronized from leader to follower during snapshot installation.
 */
public final class TestSecretKeySnapshot {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestSecretKeySnapshot.class);
  private static final long SNAPSHOT_THRESHOLD = 100;
  private static final int LOG_PURGE_GAP = 100;
  public static final int ROTATE_CHECK_DURATION_MS = 1_000;
  public static final int ROTATE_DURATION_MS = 30_000;
  public static final int EXPIRY_DURATION_MS = 61_000;

  private MiniKdc miniKdc;
  private OzoneConfiguration conf;
  @TempDir
  private File workDir;
  private File ozoneKeytab;
  private File spnegoKeytab;
  private MiniOzoneHAClusterImpl cluster;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");

    ExitUtils.disableSystemExit();

    startMiniKdc();
    setSecureConfig();
    createCredentialsInKDC();

    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_ENABLED, true);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
        SNAPSHOT_THRESHOLD);

    conf.set(HDDS_SECRET_KEY_ROTATE_CHECK_DURATION,
        ROTATE_CHECK_DURATION_MS + "ms");
    conf.set(HDDS_SECRET_KEY_ROTATE_DURATION, ROTATE_DURATION_MS + "ms");
    conf.set(HDDS_SECRET_KEY_EXPIRY_DURATION, EXPIRY_DURATION_MS + "ms");
    conf.set(DELEGATION_TOKEN_MAX_LIFETIME_KEY, ROTATE_DURATION_MS + "ms");
    conf.set(DELEGATION_REMOVER_SCAN_INTERVAL_KEY, ROTATE_CHECK_DURATION_MS + "ms");

    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder
        .setSCMServiceId("TestSecretKeySnapshot")
        .setSCMServiceId("SCMServiceId")
        .setNumOfStorageContainerManagers(3)
        .setNumOfActiveSCMs(2)
        .setNumOfOzoneManagers(1)
        .setNumDatanodes(1);

    cluster = builder.build();
    cluster.waitForClusterToBeReady();
  }

  @AfterEach
  public void stop() {
    miniKdc.stop();
    IOUtils.closeQuietly(cluster);
  }

  private void createCredentialsInKDC() throws Exception {
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    SCMHTTPServerConfig httpServerConfig =
        conf.getObject(SCMHTTPServerConfig.class);
    createPrincipal(ozoneKeytab, scmConfig.getKerberosPrincipal());
    createPrincipal(spnegoKeytab, httpServerConfig.getKerberosPrincipal());
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

    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
  }

  @Test
  public void testInstallSnapshot() throws Exception {
    // Get the leader SCM
    StorageContainerManager leaderSCM = cluster.getScmLeader();
    assertNotNull(leaderSCM);
    // Find the inactive SCM
    String followerId = cluster.getInactiveSCM().next().getSCMNodeId();

    StorageContainerManager followerSCM = cluster.getSCM(followerId);

    // wait until leader SCM got enough secret keys.
    SecretKeyManager leaderSecretKeyManager = leaderSCM.getSecretKeyManager();
    GenericTestUtils.waitFor(
        () -> leaderSecretKeyManager.getSortedKeys().size() >= 2,
        ROTATE_CHECK_DURATION_MS, EXPIRY_DURATION_MS);

    writeToIncreaseLogIndex(leaderSCM, 200);
    ManagedSecretKey currentKeyInLeader =
        leaderSecretKeyManager.getCurrentSecretKey();

    // Start the inactive SCM. Install Snapshot will happen as part
    // of setConfiguration() call to ratis leader and the follower will catch
    // up
    LOG.info("Starting follower...");
    cluster.startInactiveSCM(followerId);

    // The recently started  should be lagging behind the leader .
    SCMStateMachine followerSM =
        followerSCM.getScmHAManager().getRatisServer().getSCMStateMachine();

    // Wait & retry for follower to update transactions to leader
    // snapshot index.
    // Timeout error if follower does not load update within 3s
    GenericTestUtils.waitFor(() ->
        followerSM.getLastAppliedTermIndex().getIndex() >= 200,
        100, 3000);
    long followerLastAppliedIndex =
        followerSM.getLastAppliedTermIndex().getIndex();
    assertThat(followerLastAppliedIndex).isGreaterThanOrEqualTo(200);
    assertFalse(followerSM.getLifeCycleState().isPausingOrPaused());

    // Verify that the follower has the secret keys created
    // while it was inactive.
    SecretKeyManager followerSecretKeyManager =
        followerSCM.getSecretKeyManager();
    assertTrue(followerSecretKeyManager.isInitialized());
    List<ManagedSecretKey> followerKeys =
        followerSecretKeyManager.getSortedKeys();
    LOG.info("Follower secret keys after snapshot: {}", followerKeys);
    assertThat(followerKeys.size()).isGreaterThanOrEqualTo(2);
    assertThat(followerKeys).contains(currentKeyInLeader);
    assertEquals(leaderSecretKeyManager.getSortedKeys(), followerKeys);

    // Wait for the next rotation, assert that the updates can be synchronized
    // normally post snapshot.
    ManagedSecretKey currentKeyPostSnapshot =
        leaderSecretKeyManager.getCurrentSecretKey();
    GenericTestUtils.waitFor(() ->
            !leaderSecretKeyManager.getCurrentSecretKey()
                .equals(currentKeyPostSnapshot),
        ROTATE_CHECK_DURATION_MS, ROTATE_DURATION_MS);
    List<ManagedSecretKey> latestLeaderKeys =
        leaderSecretKeyManager.getSortedKeys();
    GenericTestUtils.waitFor(() ->
            latestLeaderKeys.equals(
            followerSecretKeyManager.getSortedKeys()),
        ROTATE_CHECK_DURATION_MS, ROTATE_DURATION_MS);
  }

  private List<ContainerInfo> writeToIncreaseLogIndex(
      StorageContainerManager scm, long targetLogIndex)
      throws IOException, InterruptedException, TimeoutException {
    List<ContainerInfo> containers = new ArrayList<>();
    SCMStateMachine stateMachine =
        scm.getScmHAManager().getRatisServer().getSCMStateMachine();
    long logIndex = scm.getScmHAManager().getRatisServer().getSCMStateMachine()
        .getLastAppliedTermIndex().getIndex();
    while (logIndex <= targetLogIndex) {
      containers.add(scm.getContainerManager()
          .allocateContainer(
              RatisReplicationConfig.getInstance(ReplicationFactor.ONE),
              this.getClass().getName()));
      Thread.sleep(100);
      logIndex = stateMachine.getLastAppliedTermIndex().getIndex();
    }
    return containers;
  }

}

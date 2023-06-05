/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;

import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test OzoneManager list volume operation under combinations of configs
 * in secure mode.
 */
public class TestOzoneManagerListVolumesSecure {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestOzoneManagerListVolumesSecure.class);

  @Rule
  public Timeout timeout = Timeout.seconds(1200);
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private String realm;
  private OzoneConfiguration conf;
  private File workDir;
  private MiniKdc miniKdc;
  private OzoneManager om;
  private static final String OM_CERT_SERIAL_ID = "9879877970576";

  private final String adminUser = "om";
  private String adminPrincipal;
  private String adminPrincipalInOtherHost;
  private File adminKeytab;
  private File adminKeytabInOtherHost;
  private UserGroupInformation adminUGI;
  private UserGroupInformation adminInOtherHostUGI;

  private final String user1 = "user1";
  private final String user2 = "user2";
  private String userPrincipal1;
  private String userPrincipal2;
  private File userKeytab1;
  private File userKeytab2;
  private UserGroupInformation userUGI1;
  private UserGroupInformation userUGI2;

  @Before
  public void init() throws Exception {
    this.conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");
    conf.set(OZONE_SECURITY_ENABLED_KEY, "true");
    conf.set("hadoop.security.authentication", "kerberos");

    this.workDir = folder.newFolder();

    startMiniKdc();
    this.realm = miniKdc.getRealm();
    createPrincipals();

    UserGroupInformation.setConfiguration(this.conf);
    this.userUGI1 = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        userPrincipal1, userKeytab1.getAbsolutePath());
    this.userUGI2 = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        userPrincipal2, userKeytab2.getAbsolutePath());
    this.adminInOtherHostUGI = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI(adminPrincipalInOtherHost,
            adminKeytabInOtherHost.getAbsolutePath());
    this.adminUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        adminPrincipal, adminKeytab.getAbsolutePath());

    // loginUser is the user running this test.
    UserGroupInformation.setLoginUser(this.adminUGI);
  }

  private void startMiniKdc() throws Exception {
    Properties securityProperties = MiniKdc.createConf();
    miniKdc = new MiniKdc(securityProperties, this.workDir);
    miniKdc.start();
  }

  private void stopMiniKdc() {
    miniKdc.stop();
  }

  private void createPrincipals() throws Exception {
    String host = InetAddress.getLocalHost().getCanonicalHostName().
        toLowerCase();
    String hostAndRealm = host + "@" + this.realm;
    this.adminPrincipal = adminUser + "/" + hostAndRealm;
    this.adminPrincipalInOtherHost = adminUser + "/otherhost@" + this.realm;
    this.adminKeytab = new File(workDir, adminUser + ".keytab");
    this.adminKeytabInOtherHost = new File(workDir, adminUser +
        "InOtherHost.keytab");
    createPrincipal(this.adminKeytab, adminPrincipal);
    createPrincipal(this.adminKeytabInOtherHost, adminPrincipalInOtherHost);

    this.userPrincipal1 = this.user1 + "/" + hostAndRealm;
    this.userPrincipal2 = this.user2 + "/" + hostAndRealm;
    this.userKeytab1  = new File(workDir, this.user1 + ".keytab");
    this.userKeytab2  = new File(workDir, this.user2 + ".keytab");
    createPrincipal(this.userKeytab1, userPrincipal1);
    createPrincipal(this.userKeytab2, userPrincipal2);
  }

  private void createPrincipal(File keytab, String... principal)
      throws Exception {
    miniKdc.createPrincipal(keytab, principal);
  }

  @After
  public void stop() {
    try {
      stopMiniKdc();

      if (om != null) {
        om.stop();
        om.join();
      }

      if (workDir != null) {
        FileUtils.deleteDirectory(workDir);
      }
    } catch (Exception e) {
      LOG.error("Failed to stop TestSecureOzoneCluster", e);
    }
  }

  /**
   * Setup test environment.
   */
  private void setupEnvironment(boolean aclEnabled,
      boolean volListAllAllowed) throws Exception {
    Path omPath = Paths.get(workDir.getPath(), "om-meta");
    conf.set(OZONE_METADATA_DIRS, omPath.toString());

    // Use native impl here, default impl doesn't do actual checks
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    conf.setBoolean(OZONE_ACL_ENABLED, aclEnabled);
    conf.setBoolean(OZONE_OM_VOLUME_LISTALL_ALLOWED, volListAllAllowed);
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, adminPrincipal);
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, adminKeytab.getAbsolutePath());

    OzoneManager.setUgi(this.adminUGI);

    OMStorage omStore = new OMStorage(conf);
    //omStore.setClusterId("testClusterId");
    omStore.setOmCertSerialId(OM_CERT_SERIAL_ID);
    // writes the version file properties
    omStore.initialize();
    OzoneManager.setTestSecureOmFlag(true);

    om = OzoneManager.createOm(conf);
    om.setCertClient(new CertificateClientTestImpl(conf));
    om.start();

    // Get OM client
    OzoneManagerProtocolClientSideTranslatorPB omClient =
        new OzoneManagerProtocolClientSideTranslatorPB(
            OmTransportFactory.create(conf, this.adminUGI, null),
            RandomStringUtils.randomAscii(5));

    // Create volume with ACL
    /* r = READ, w = WRITE, c = CREATE, d = DELETE
       l = LIST, a = ALL, n = NONE, x = READ_ACL, y = WRITE_ACL */
    String aclUser1All = "user:user1:a";
    String aclUser2All = "user:user2:a";
    String aclWorldAll = "world::a";
    createVolumeWithOwnerAndAcl(omClient, "volume1", user1, aclUser1All);
    createVolumeWithOwnerAndAcl(omClient, "volume2", user2, aclUser2All);
    createVolumeWithOwnerAndAcl(omClient, "volume3", user1, aclUser2All);
    createVolumeWithOwnerAndAcl(omClient, "volume4", user2, aclUser1All);
    createVolumeWithOwnerAndAcl(omClient, "volume5", user1, aclWorldAll);
    createVolumeWithOwnerAndAcl(omClient, "volume6", adminUser, null);
    omClient.close();
  }

  private void createVolumeWithOwnerAndAcl(
      OzoneManagerProtocolClientSideTranslatorPB client, String volumeName,
      String ownerName, String aclString) throws IOException {
    // Create volume use adminUgi
    OmVolumeArgs.Builder builder =
        OmVolumeArgs.newBuilder().setVolume(volumeName).setAdminName(adminUser);
    if (!Strings.isNullOrEmpty(ownerName)) {
      builder.setOwnerName(ownerName);
    }
    client.createVolume(builder.build());

    if (!Strings.isNullOrEmpty(aclString)) {
      OzoneObj obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volumeName)
          .setResType(OzoneObj.ResourceType.VOLUME).setStoreType(OZONE).build();
      Assert.assertTrue(client.setAcl(obj, OzoneAcl.parseAcls(aclString)));
    }
  }

  /**
   * Helper function to reduce code redundancy for test checks with each user
   * under different config combination.
   */
  private void checkUser(String userName, List<String> expectVol,
      boolean expectListAllSuccess) throws IOException {

    OzoneManagerProtocolClientSideTranslatorPB client =
        new OzoneManagerProtocolClientSideTranslatorPB(
            OmTransportFactory.create(conf,
                UserGroupInformation.getCurrentUser(), null),
            RandomStringUtils.randomAscii(5));

    // `ozone sh volume list` shall return volumes with LIST permission of user.
    List<OmVolumeArgs> volumeList;
    try {
      volumeList = client.listVolumeByUser(userName, "", "", 100);
      Set<String> accessibleVolumes = new HashSet<>();
      for (OmVolumeArgs v : volumeList) {
        String volumeName = v.getVolume();
        accessibleVolumes.add(volumeName);
      }
      Assert.assertEquals(new HashSet<>(expectVol), accessibleVolumes);
    } catch (OMException e) {
      if (!expectListAllSuccess &&
          e.getResult() == OMException.ResultCodes.PERMISSION_DENIED) {
        return;
      }
      throw e;
    } finally {
      client.close();
    }

    // `ozone sh volume list --all` returns all volumes,
    //  or throws exception (for non-admin if acl enabled & listall
    //  disallowed).
    try {
      volumeList = client.listAllVolumes("volume", "", 100);
      Assert.assertEquals(6, volumeList.size());
      Assert.assertTrue(expectListAllSuccess);
    } catch (OMException ex) {
      if (!expectListAllSuccess &&
          ex.getResult() == OMException.ResultCodes.PERMISSION_DENIED) {
        return;
      }
      throw ex;
    } finally {
      client.close();
    }
  }

  private static void doAs(UserGroupInformation ugi,
      Callable<Boolean> callable) {
    // Some thread (eg: HeartbeatEndpointTask) will use the login ugi,
    // so we could not use loginUserFromKeytabAndReturnUGI to switch user.
    Assert.assertEquals(true, ugi.doAs((PrivilegedAction) () -> {
      try {
        return callable.call();
      } catch (Throwable ex) {
        LOG.warn("DoAs Failed, caused by ", ex);
        return false;
      }
    }));
  }

  /**
   * Check if listVolume of other users than the login user works as expected.
   * ozone.om.volume.listall.allowed = true
   * Everyone should be able to list other users' volumes with this config.
   */
  @Test
  public void testListVolumeWithOtherUsersListAllAllowed() throws Exception {
    setupEnvironment(true, true);

    // Login as user1, list other users' volumes
    doAs(userUGI1, () -> {
      checkUser(user2, Arrays.asList("volume2", "volume3", "volume4",
          "volume5"), true);
      checkUser(adminUser, Arrays
          .asList("volume1", "volume2", "volume3", "volume4", "volume5",
              "volume6", "s3v"), true);
      return true;
    });

    // Login as user2, list other users' volumes
    doAs(userUGI2, () -> {
      checkUser(user1, Arrays.asList("volume1", "volume3", "volume4",
          "volume5"), true);
      checkUser(adminUser, Arrays
          .asList("volume1", "volume2", "volume3", "volume4", "volume5",
              "volume6", "s3v"), true);
      return true;
    });

    // Login as admin, list other users' volumes
    doAs(adminUGI, () -> {
      checkUser(user1, Arrays.asList("volume1", "volume3", "volume4",
          "volume5"), true);
      checkUser(user2, Arrays.asList("volume2", "volume3", "volume4",
          "volume5"), true);
      return true;
    });

    // Login as admin in other host, list other users' volumes
    doAs(adminInOtherHostUGI, () -> {
      checkUser(user1, Arrays.asList("volume1", "volume3",
          "volume4", "volume5"), true);
      checkUser(user2, Arrays.asList("volume2", "volume3",
          "volume4", "volume5"), true);
      return true;
    });
  }

  /**
   * Check if listVolume of other users than the login user works as expected.
   * ozone.om.volume.listall.allowed = false
   * Only admin should be able to list other users' volumes with this config.
   */
  @Test
  public void testListVolumeWithOtherUsersListAllDisallowed() throws Exception {
    setupEnvironment(true, false);

    // Login as user1, list other users' volumes, expect failure
    doAs(userUGI1, () -> {
      checkUser(user2, Arrays.asList("volume2", "volume3", "volume4",
          "volume5"), false);
      checkUser(adminUser, Arrays.asList("volume1", "volume2", "volume3",
              "volume4", "volume5", "volume6", "s3v"), false);
      return true;
    });

    // Login as user2, list other users' volumes, expect failure
    doAs(userUGI2, () -> {
      checkUser(user1, Arrays.asList("volume1", "volume3", "volume4",
          "volume5"), false);
      checkUser(adminUser,
          Arrays.asList("volume1", "volume2", "volume3",
              "volume4", "volume5", "volume6", "s3v"), false);
      return true;
    });

    // While admin should be able to list volumes just fine.
    doAs(adminUGI, () -> {
      checkUser(user1, Arrays.asList("volume1", "volume3", "volume4",
          "volume5"), true);
      checkUser(user2, Arrays.asList("volume2", "volume3", "volume4",
          "volume5"), true);
      return true;
    });

    // While admin in other host should be able to list volumes just fine.
    doAs(adminInOtherHostUGI, () -> {
      checkUser(user1, Arrays.asList("volume1", "volume3",
          "volume4", "volume5"), true);
      checkUser(user2, Arrays.asList("volume2", "volume3",
          "volume4", "volume5"), true);
      return true;
    });
  }

  @Test
  public void testAclEnabledListAllAllowed() throws Exception {
    setupEnvironment(true, true);

    // Login as user1, list their own volumes
    doAs(userUGI1, () -> {
      checkUser(user1, Arrays.asList("volume1", "volume3", "volume4",
          "volume5"), true);
      return true;
    });

    // Login as user2, list their own volumes
    doAs(userUGI2, () -> {
      checkUser(user2, Arrays.asList("volume2", "volume3", "volume4",
          "volume5"), true);
      return true;
    });

    // Login as admin, list their own volumes
    doAs(adminUGI, () -> {
      checkUser(adminUser, Arrays.asList("volume1", "volume2", "volume3",
          "volume4", "volume5", "volume6", "s3v"), true);
      return true;
    });

    // Login as admin in other host, list their own volumes
    doAs(adminInOtherHostUGI, () -> {
      checkUser(adminUser, Arrays.asList("volume1", "volume2",
          "volume3", "volume4", "volume5", "volume6", "s3v"), true);
      return true;
    });
  }

  @Test
  public void testAclEnabledListAllDisallowed() throws Exception {
    setupEnvironment(true, false);

    // Login as user1, list their own volumes
    doAs(userUGI1, () -> {
      checkUser(user1, Arrays.asList("volume1", "volume3", "volume4",
          "volume5"), false);
      return true;
    });

    // Login as user2, list their own volumes
    doAs(userUGI2, () -> {
      checkUser(userPrincipal2, Arrays.asList("volume2", "volume3",
          "volume4", "volume5"), false);
      return true;
    });


    // Login as admin, list their own volumes
    doAs(adminUGI, () -> {
      checkUser(adminPrincipal, Arrays.asList("volume1", "volume2",
          "volume3", "volume4", "volume5", "volume6", "s3v"), true);
      return true;
    });

    // Login as admin in other host, list their own volumes
    doAs(adminInOtherHostUGI, () -> {
      checkUser(adminPrincipalInOtherHost, Arrays.asList(
          "volume1", "volume2", "volume3", "volume4", "volume5", "volume6",
          "s3v"), true);
      return true;
    });
  }

  @Test
  public void testAclDisabledListAllAllowed() throws Exception {
    setupEnvironment(false, true);

      // Login as user1, list their own volumes
    doAs(userUGI1, () -> {
      checkUser(user1, Arrays.asList("volume1", "volume3", "volume5"),
          true);
      return true;
    });

    // Login as user2, list their own volumes
    doAs(userUGI2, () -> {
      checkUser(user2, Arrays.asList("volume2", "volume4"),
          true);
      return true;
    });

    doAs(adminUGI, () -> {
      checkUser(adminUser, Arrays.asList("volume6", "s3v"), true);
      return true;
    });

    // Login as admin in other host, list their own volumes
    doAs(adminInOtherHostUGI, () -> {
      checkUser(adminUser, Arrays.asList("volume6", "s3v"),
          true);
      return true;
    });
  }

  @Test
  public void testAclDisabledListAllDisallowed() throws Exception {
    setupEnvironment(false, false);

    // Login as user1, list their own volumes
    doAs(userUGI1, () -> {
      checkUser(user1, Arrays.asList("volume1", "volume3", "volume5"),
          true);
      return true;
    });

    // Login as user2, list their own volumes
    doAs(userUGI2, () -> {
      checkUser(user2, Arrays.asList("volume2", "volume4"),
          true);
      return true;
    });

    doAs(adminUGI, () -> {
      checkUser(adminUser, Arrays.asList("volume6", "s3v"), true);
      return true;
    });

    // Login as admin in other host, list their own volumes
    doAs(adminInOtherHostUGI, () -> {
      checkUser(adminUser, Arrays.asList("volume6", "s3v"),
          true);
      return true;
    });
  }

}

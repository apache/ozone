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

package org.apache.ozone.test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Shared MiniKdc / Kerberos setup for secure cluster integration tests (HDDS-15913).
 */
public abstract class KerberosTests {

  private MiniKdc miniKdc;
  private OzoneConfiguration conf;

  private File workDir;

  /** Service keytab (scm.keytab); shared by SCM/OM/DN when useSharedServicePrincipal(). */
  private File ozoneKeytab;
  private File spnegoKeytab;
  private File testUserKeytab;
  private String testUserPrincipal;
  /** e.g. scm/host@REALM when useSharedServicePrincipal(). */
  private String ozonePrincipal;

  /** Separate OM keytab, only used when useSharedServicePrincipal() is false. */
  private File omKeytab;

  protected OzoneConfiguration getConf() {
    return conf;
  }

  protected OzoneConfiguration createOzoneConfig() {
    return new OzoneConfiguration();
  }

  protected File getOzoneKeytab() {
    return ozoneKeytab;
  }

  protected String getOzonePrincipal() {
    return ozonePrincipal;
  }

  protected File getTestUserKeytab() {
    return testUserKeytab;
  }

  protected String getTestUserPrincipal() {
    return testUserPrincipal;
  }

  protected void initKerberos() throws Exception {
    if (conf == null) {
      conf = createOzoneConfig();
    }
    startMiniKdc();
    setSecureConfig();
    createCredentialsInKDC();
  }

  protected void startMiniKdc() throws Exception {
    if (workDir == null) {
      workDir = Files.createTempDirectory("kerberos").toFile();
    }
    Properties securityProperties = MiniKdc.createConf();
    miniKdc = new MiniKdc(securityProperties, workDir);
    miniKdc.start();
  }

  protected void stopMiniKdc() {
    if (miniKdc != null) {
      miniKdc.stop();
    }
    FileUtils.deleteQuietly(workDir);
  }

  protected void createPrincipal(File keytab, String... principal)
      throws Exception {
    miniKdc.createPrincipal(keytab, principal);
  }

  protected void setSecureConfig() throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    String host = InetAddress.getLocalHost().getCanonicalHostName()
        .toLowerCase();

    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());

    String curUser = UserGroupInformation.getCurrentUser().getUserName();
    conf.set(OZONE_ADMINISTRATORS, curUser);

    String realm = miniKdc.getRealm();
    String hostAndRealm = host + "@" + realm;

    ozoneKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");

    if (useSharedServicePrincipal()) {
      ozonePrincipal = "scm/" + hostAndRealm;
      conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
      conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
      conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, ozoneKeytab.getAbsolutePath());
      conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, ozoneKeytab.getAbsolutePath());
      conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
      conf.set(HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY,
          ozoneKeytab.getAbsolutePath());
    } else {
      ozonePrincipal = "scm/" + hostAndRealm;
      conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
      conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, ozoneKeytab.getAbsolutePath());
      omKeytab = new File(workDir, "om.keytab");
      conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "om/" + hostAndRealm);
      conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, omKeytab.getAbsolutePath());
    }

    conf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_SCM/" + hostAndRealm);
    conf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_OM/" + hostAndRealm);
    conf.set(HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
    conf.set(OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE,
        spnegoKeytab.getAbsolutePath());

    if (createTestUserPrincipal()) {
      testUserKeytab = new File(workDir, "testuser.keytab");
      testUserPrincipal = "test@" + realm;
    }

    if (enableSecurityAuthorizationByDefault()) {
      conf.setBoolean(HADOOP_SECURITY_AUTHORIZATION, true);
    }
  }

  protected void createCredentialsInKDC() throws Exception {
    SCMHTTPServerConfig httpServerConfig =
        conf.getObject(SCMHTTPServerConfig.class);
    createPrincipal(ozoneKeytab, conf.get(HDDS_SCM_KERBEROS_PRINCIPAL_KEY));
    if (!useSharedServicePrincipal()) {
      createPrincipal(omKeytab, conf.get(OZONE_OM_KERBEROS_PRINCIPAL_KEY));
    }
    createPrincipal(spnegoKeytab, httpServerConfig.getKerberosPrincipal());
    if (createTestUserPrincipal()) {
      createPrincipal(testUserKeytab, testUserPrincipal);
    }
  }

  /** Whether SCM/OM (and optionally DN) share a single "scm/..." principal and keytab. */
  protected boolean useSharedServicePrincipal() {
    return true;
  }

  protected boolean createTestUserPrincipal() {
    return true;
  }

  protected boolean enableSecurityAuthorizationByDefault() {
    return true;
  }
}

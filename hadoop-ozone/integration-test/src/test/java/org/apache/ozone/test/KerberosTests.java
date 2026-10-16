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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
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
  private final OzoneConfiguration conf = new OzoneConfiguration();

  private File workDir;

  /** Service keytab (scm.keytab); shared by SCM/OM/DN when using {@link #configureSharedServicePrincipal}. */
  private File ozoneKeytab;
  private File spnegoKeytab;
  private File testUserKeytab;
  private String testUserPrincipal;
  /** e.g. scm/host@REALM. */
  private String ozonePrincipal;

  /** Separate OM keytab, only used with {@link #configureSeparateServicePrincipals}. */
  private File omKeytab;

  protected OzoneConfiguration getConf() {
    return conf;
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
    configureSecurityBasics();
    String host = InetAddress.getLocalHost().getCanonicalHostName()
        .toLowerCase();
    String hostAndRealm = host + "@" + getRealm();
    configureSharedServicePrincipal(hostAndRealm);
    configureSpnegoPrincipal(hostAndRealm);
    createTestUserCredentials();
    conf.setBoolean(HADOOP_SECURITY_AUTHORIZATION, true);
  }

  /** Mints the principals for whichever keytabs {@link #setSecureConfig()} configured. */
  protected void createCredentialsInKDC() throws Exception {
    createPrincipal(ozoneKeytab, conf.get(HDDS_SCM_KERBEROS_PRINCIPAL_KEY));
    if (omKeytab != null) {
      createPrincipal(omKeytab, conf.get(OZONE_OM_KERBEROS_PRINCIPAL_KEY));
    }
    SCMHTTPServerConfig httpServerConfig = conf.getObject(SCMHTTPServerConfig.class);
    createPrincipal(spnegoKeytab, httpServerConfig.getKerberosPrincipal());
    if (testUserKeytab != null) {
      createPrincipal(testUserKeytab, testUserPrincipal);
    }
  }

  protected String getRealm() {
    return miniKdc.getRealm();
  }

  /** Security basics common to every subclass's setSecureConfig(). */
  protected void configureSecurityBasics() throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");
    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());
    String curUser = UserGroupInformation.getCurrentUser().getUserName();
    conf.set(OZONE_ADMINISTRATORS, curUser);
  }

  /** SCM/OM/DN share a single "scm/..." principal and keytab. */
  protected void configureSharedServicePrincipal(String hostAndRealm) {
    ozonePrincipal = "scm/" + hostAndRealm;
    ozoneKeytab = new File(workDir, "scm.keytab");
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, ozoneKeytab.getAbsolutePath());
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, ozoneKeytab.getAbsolutePath());
    conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
    conf.set(HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY,
        ozoneKeytab.getAbsolutePath());
  }

  /** SCM and OM use separate "scm/..." and "om/..." principals and keytabs. */
  protected void configureSeparateServicePrincipals(String hostAndRealm) {
    ozonePrincipal = "scm/" + hostAndRealm;
    ozoneKeytab = new File(workDir, "scm.keytab");
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, ozoneKeytab.getAbsolutePath());
    omKeytab = new File(workDir, "om.keytab");
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "om/" + hostAndRealm);
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, omKeytab.getAbsolutePath());
  }

  protected void configureSpnegoPrincipal(String hostAndRealm) {
    spnegoKeytab = new File(workDir, "http.keytab");
    conf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_SCM/" + hostAndRealm);
    conf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_OM/" + hostAndRealm);
    conf.set(HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
    conf.set(OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE,
        spnegoKeytab.getAbsolutePath());
  }

  protected void createTestUserCredentials() {
    testUserKeytab = new File(workDir, "testuser.keytab");
    testUserPrincipal = "test@" + getRealm();
  }
}

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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType.KERBEROS_TRUSTED;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CommonNameTestUtil.LONG_HOST_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertPath;
import java.security.cert.CertificateFactory;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmNodeDetailsProto;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the commonName that {@link SCMSecurityProtocolServer} passes to the CA for each certificate RPC, which
 * replaces a CSR commonName longer than the RFC 5280 ub-common-name (64) from a client that predates
 * {@link CertificateSignRequest#commonName}.
 */
public class TestSCMSecurityProtocolServer {
  private static final String LONG_SUBJECT = "user@" + LONG_HOST_NAME;
  private static final String HOST_NAME = "host1.example.com";

  private final String clusterId = UUID.randomUUID().toString();
  private CertificateServer rootCA;
  private CertificateServer subCA;
  private SCMSecurityProtocolServer server;
  private String csr;

  @BeforeEach
  public void setUp(@TempDir Path metaDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, metaDir.toString());
    conf.set(OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, "dn/_HOST@EXAMPLE.COM");
    conf.set("ozone.om.kerberos.principal", "om/_HOST@EXAMPLE.COM");
    conf.set(OZONE_RECON_KERBEROS_PRINCIPAL_KEY, "recon/_HOST@EXAMPLE.COM");
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, "scm/_HOST@EXAMPLE.COM");
    SecurityConfig securityConfig = new SecurityConfig(conf);

    KeyPair keyPair = new HDDSKeyGenerator(securityConfig).generateKey();
    CertPath certPath = CertificateFactory.getInstance("X.509").generateCertPath(Collections.singletonList(
        KeyStoreTestUtil.generateCertificate("CN=Test", keyPair, 1, securityConfig.getSignatureAlgo())));
    rootCA = mock(CertificateServer.class);
    subCA = mock(CertificateServer.class);
    for (CertificateServer ca : new CertificateServer[] {rootCA, subCA}) {
      when(ca.requestCertificate(any(), any(), any(), anyString(), any()))
          .thenReturn(CompletableFuture.completedFuture(certPath));
    }

    StorageContainerManager scm = mock(StorageContainerManager.class);
    SequenceIdGenerator sequenceIdGen = mock(SequenceIdGenerator.class);
    when(sequenceIdGen.getNextId(any())).thenReturn(2L);
    when(scm.getSequenceIdGen()).thenReturn(sequenceIdGen);
    when(scm.getClusterId()).thenReturn(clusterId);
    server = new SCMSecurityProtocolServer(conf, rootCA, subCA, null, scm, null);

    csr = new CertificateSignRequest.Builder()
        .setSubject(LONG_SUBJECT)
        .setScmID(UUID.randomUUID().toString())
        .setClusterID(clusterId)
        .setKey(keyPair)
        .setConfiguration(securityConfig)
        .build()
        .toEncodedFormat();
  }

  @AfterEach
  public void tearDown() {
    server.stop();
  }

  @Test
  public void testDataNodeCertificate() throws Exception {
    String uuid = UUID.randomUUID().toString();
    DatanodeDetailsProto dn = DatanodeDetailsProto.newBuilder()
        .setUuid(uuid).setIpAddress("127.0.0.1").setHostName(HOST_NAME).build();
    as("dn", () -> server.getDataNodeCertificate(dn, csr));
    verify(subCA).requestCertificate(any(), eq(KERBEROS_TRUSTED), eq(NodeType.DATANODE), anyString(),
        eq("dn@" + uuid));
  }

  @Test
  public void testOMCertificate() throws Exception {
    String uuid = UUID.randomUUID().toString();
    OzoneManagerDetailsProto om = OzoneManagerDetailsProto.newBuilder()
        .setUuid(uuid).setIpAddress("127.0.0.1").setHostName(HOST_NAME).build();
    as("om", () -> server.getOMCertificate(om, csr));
    verify(subCA).requestCertificate(any(), eq(KERBEROS_TRUSTED), eq(NodeType.OM), anyString(), eq("om@" + uuid));
  }

  @Test
  public void testReconCertificate() throws Exception {
    String uuid = UUID.randomUUID().toString();
    NodeDetailsProto recon = NodeDetailsProto.newBuilder()
        .setUuid(uuid).setClusterId(clusterId).setHostName(HOST_NAME).setNodeType(NodeType.RECON).build();
    as("recon", () -> server.getCertificate(recon, csr));
    verify(subCA).requestCertificate(any(), eq(KERBEROS_TRUSTED), eq(NodeType.RECON), anyString(),
        eq("recon@" + uuid));
  }

  @Test
  public void testSCMCertificate() throws Exception {
    String scmId = UUID.randomUUID().toString();
    ScmNodeDetailsProto scm = ScmNodeDetailsProto.newBuilder()
        .setScmNodeId(scmId).setClusterId(clusterId).setHostName(HOST_NAME).build();
    as("scm", () -> server.getSCMCertificate(scm, csr));
    verify(rootCA).requestCertificate(any(), eq(KERBEROS_TRUSTED), eq(NodeType.SCM), anyString(),
        eq("scm-sub@" + scmId));
  }

  /** Calls {@code action} as the Kerberos service user of {@code service}, the only user allowed its certificate. */
  private static void as(String service, PrivilegedExceptionAction<String> action) throws Exception {
    UserGroupInformation.createRemoteUser(service + "/" + HOST_NAME + "@EXAMPLE.COM").doAs(action);
  }
}

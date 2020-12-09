/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.server;

import com.google.protobuf.BlockingService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.protocol.SCMSecurityProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.security.KerberosInfo;

import org.bouncycastle.cert.X509CertificateHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType.KERBEROS_TRUSTED;

/**
 * The protocol used to perform security related operations with SCM.
 */
@KerberosInfo(
    serverPrincipal = ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public class SCMSecurityProtocolServer implements SCMSecurityProtocol {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SCMSecurityProtocolServer.class);
  private final CertificateServer certificateServer;
  private final RPC.Server rpcServer;
  private final InetSocketAddress rpcAddress;
  private final ProtocolMessageMetrics metrics;

  SCMSecurityProtocolServer(OzoneConfiguration conf,
      CertificateServer certificateServer) throws IOException {
    this.certificateServer = certificateServer;
    final int handlerCount =
        conf.getInt(ScmConfigKeys.OZONE_SCM_SECURITY_HANDLER_COUNT_KEY,
            ScmConfigKeys.OZONE_SCM_SECURITY_HANDLER_COUNT_DEFAULT);
    rpcAddress = HddsServerUtil
        .getScmSecurityInetAddress(conf);
    // SCM security service RPC service.
    RPC.setProtocolEngine(conf, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);
    metrics = new ProtocolMessageMetrics("ScmSecurityProtocol",
        "SCM Security protocol metrics",
        SCMSecurityProtocolProtos.Type.values());
    BlockingService secureProtoPbService =
        SCMSecurityProtocolProtos.SCMSecurityProtocolService
            .newReflectiveBlockingService(
                new SCMSecurityProtocolServerSideTranslatorPB(this, metrics));
    this.rpcServer =
        StorageContainerManager.startRpcServer(
            conf,
            rpcAddress,
            SCMSecurityProtocolPB.class,
            secureProtoPbService,
            handlerCount);
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      rpcServer.refreshServiceAcl(conf, SCMPolicyProvider.getInstance());
    }
  }

  /**
   * Get SCM signed certificate for DataNode.
   *
   * @param dnDetails   - DataNode Details.
   * @param certSignReq - Certificate signing request.
   * @return String         - SCM signed pem encoded certificate.
   */
  @Override
  public String getDataNodeCertificate(
      DatanodeDetailsProto dnDetails,
      String certSignReq) throws IOException {
    LOGGER.info("Processing CSR for dn {}, UUID: {}", dnDetails.getHostName(),
        dnDetails.getUuid());
    Objects.requireNonNull(dnDetails);
    Future<X509CertificateHolder> future =
        certificateServer.requestCertificate(certSignReq,
            KERBEROS_TRUSTED);

    try {
      return CertificateCodec.getPEMEncodedString(future.get());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("getDataNodeCertificate operation failed. ", e);
    } catch (ExecutionException e) {
      throw new IOException("getDataNodeCertificate operation failed. ", e);
    }
  }

  /**
   * Get SCM signed certificate for OM.
   *
   * @param omDetails   - OzoneManager Details.
   * @param certSignReq - Certificate signing request.
   * @return String         - SCM signed pem encoded certificate.
   */
  @Override
  public String getOMCertificate(OzoneManagerDetailsProto omDetails,
      String certSignReq) throws IOException {
    LOGGER.info("Processing CSR for om {}, UUID: {}", omDetails.getHostName(),
        omDetails.getUuid());
    Objects.requireNonNull(omDetails);
    Future<X509CertificateHolder> future =
        certificateServer.requestCertificate(certSignReq,
            KERBEROS_TRUSTED);

    try {
      return CertificateCodec.getPEMEncodedString(future.get());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("getOMCertificate operation failed. ", e);
    } catch (ExecutionException e) {
      throw new IOException("getOMCertificate operation failed. ", e);
    }
  }

  /**
   * Get SCM signed certificate with given serial id.
   *
   * @param certSerialId - Certificate serial id.
   * @return string         - pem encoded SCM signed certificate.
   */
  @Override
  public String getCertificate(String certSerialId) throws IOException {
    LOGGER.debug("Getting certificate with certificate serial id {}",
        certSerialId);
    try {
      X509Certificate certificate =
          certificateServer.getCertificate(certSerialId);
      if (certificate != null) {
        return CertificateCodec.getPEMEncodedString(certificate);
      }
    } catch (CertificateException e) {
      throw new IOException("getCertificate operation failed. ", e);
    }
    LOGGER.debug("Certificate with serial id {} not found.", certSerialId);
    throw new IOException("Certificate not found");
  }

  /**
   * Get SCM signed certificate for OM.
   *
   * @return string         - Root certificate.
   */
  @Override
  public String getCACertificate() throws IOException {
    LOGGER.debug("Getting CA certificate.");
    try {
      return CertificateCodec.getPEMEncodedString(
          certificateServer.getCACertificate());
    } catch (CertificateException e) {
      throw new IOException("getRootCertificate operation failed. ", e);
    }
  }

  /**
   *
   * @param role            - node role: OM/SCM/DN.
   * @param startSerialId   - start certificate serial id.
   * @param count           - max number of certificates returned in a batch.
   * @param isRevoked       - whether list for revoked certs only.
   * @return
   * @throws IOException
   */
  @Override
  public List<String> listCertificate(HddsProtos.NodeType role,
      long startSerialId, int count, boolean isRevoked) throws IOException {
    List<X509Certificate> certificates =
        certificateServer.listCertificate(role, startSerialId, count,
            isRevoked);
    List<String> results = new ArrayList<>(certificates.size());
    for (X509Certificate cert : certificates) {
      try {
        String certStr = CertificateCodec.getPEMEncodedString(cert);
        results.add(certStr);
      } catch (SCMSecurityException e) {
        throw new IOException("listCertificate operation failed. ", e);
      }
    }
    return results;
  }

  public RPC.Server getRpcServer() {
    return rpcServer;
  }

  public InetSocketAddress getRpcAddress() {
    return rpcAddress;
  }

  public void start() {
    String startupMsg = StorageContainerManager.buildRpcServerStartMessage(
        "Starting RPC server for SCMSecurityProtocolServer.", getRpcAddress());
    LOGGER.info(startupMsg);
    metrics.register();
    getRpcServer().start();
  }

  public void stop() {
    try {
      LOGGER.info("Stopping the SCMSecurityProtocolServer.");
      metrics.unregister();
      getRpcServer().stop();
    } catch (Exception ex) {
      LOGGER.error("SCMSecurityProtocolServer stop failed.", ex);
    }
  }

  public void join() throws InterruptedException {
    LOGGER.trace("Join RPC server for SCMSecurityProtocolServer.");
    getRpcServer().join();
  }

}

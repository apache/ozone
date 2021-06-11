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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmNodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.update.server.SCMUpdateServiceGrpcServer;
import org.apache.hadoop.hdds.scm.update.client.UpdateServiceConfig;
import org.apache.hadoop.hdds.scm.update.server.SCMCRLStore;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.protocol.SCMSecurityProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.KerberosInfo;

import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CertificateHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.CERTIFICATE_NOT_FOUND;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.GET_CA_CERT_FAILED;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.GET_CERTIFICATE_FAILED;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.NOT_A_PRIMARY_SCM;
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
  private final CertificateServer rootCertificateServer;
  private final CertificateServer scmCertificateServer;
  private final X509Certificate rootCACertificate;
  private final RPC.Server rpcServer; // HADOOP RPC SERVER
  private final SCMUpdateServiceGrpcServer grpcUpdateServer; // gRPC SERVER
  private final InetSocketAddress rpcAddress;
  private final ProtocolMessageMetrics metrics;
  private final StorageContainerManager storageContainerManager;

  SCMSecurityProtocolServer(OzoneConfiguration conf,
      CertificateServer rootCertificateServer,
      CertificateServer scmCertificateServer,
      X509Certificate rootCACert, StorageContainerManager scm)
      throws IOException {
    this.storageContainerManager = scm;
    this.rootCertificateServer = rootCertificateServer;
    this.scmCertificateServer = scmCertificateServer;
    this.rootCACertificate = rootCACert;
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
                new SCMSecurityProtocolServerSideTranslatorPB(this,
                    scm, metrics));
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

    this.grpcUpdateServer = new SCMUpdateServiceGrpcServer(
        conf.getObject(UpdateServiceConfig.class),
        new SCMCRLStore(scmCertificateServer));
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
    return getEncodedCertToString(certSignReq, NodeType.DATANODE);
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
    return getEncodedCertToString(certSignReq, NodeType.OM);
  }


  /**
   * Get signed certificate for SCM Node.
   *
   * @param scmNodeDetails   - SCM Node Details.
   * @param certSignReq - Certificate signing request.
   * @return String         - SCM signed pem encoded certificate.
   */
  @Override
  public String getSCMCertificate(ScmNodeDetailsProto scmNodeDetails,
      String certSignReq) throws IOException {
    Objects.requireNonNull(scmNodeDetails);
    String primaryScmId =
        storageContainerManager.getScmStorageConfig().getPrimaryScmNodeId();

    if (primaryScmId != null &&
        primaryScmId.equals(storageContainerManager.getScmId())) {
      LOGGER.info("Processing CSR for scm {}, nodeId: {}",
          scmNodeDetails.getHostName(), scmNodeDetails.getScmNodeId());

      // Check clusterID
      if (!storageContainerManager.getClusterId().equals(
          scmNodeDetails.getClusterId())) {
        throw new IOException("SCM ClusterId mismatch. Peer SCM ClusterId " +
            scmNodeDetails.getClusterId() + ", primary SCM ClusterId "
            + storageContainerManager.getClusterId());
      }

      return getEncodedCertToString(certSignReq, NodeType.SCM);
    } else {
      throw new SCMSecurityException("Get SCM Certificate can be run only " +
          "primary SCM", NOT_A_PRIMARY_SCM);
    }

  }

  /**
   *  Request certificate for the specified role.
   * @param certSignReq - Certificate signing request.
   * @param nodeType - role OM/SCM/DATANODE
   * @return String         - SCM signed pem encoded certificate.
   * @throws IOException
   */
  private String getEncodedCertToString(String certSignReq, NodeType nodeType)
      throws IOException {

    Future<X509CertificateHolder> future;
    if (nodeType == NodeType.SCM) {
      future = rootCertificateServer.requestCertificate(certSignReq,
              KERBEROS_TRUSTED, nodeType);
    } else {
      future = scmCertificateServer.requestCertificate(certSignReq,
          KERBEROS_TRUSTED, nodeType);
    }
    try {
      return CertificateCodec.getPEMEncodedString(future.get());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw generateException(e, nodeType);
    } catch (ExecutionException e) {
      if (e.getCause() != null) {
        if (e.getCause() instanceof SCMSecurityException) {
          throw (SCMSecurityException) e.getCause();
        } else {
          throw generateException(e, nodeType);
        }
      } else {
        throw generateException(e, nodeType);
      }
    }
  }

  private SCMSecurityException generateException(Exception ex, NodeType role) {
    SCMSecurityException.ErrorCode errorCode;
    if (role == NodeType.SCM) {
      errorCode = SCMSecurityException.ErrorCode.GET_SCM_CERTIFICATE_FAILED;
    } else if (role == NodeType.OM) {
      errorCode = SCMSecurityException.ErrorCode.GET_OM_CERTIFICATE_FAILED;
    } else {
      errorCode = SCMSecurityException.ErrorCode.GET_DN_CERTIFICATE_FAILED;
    }
    return new SCMSecurityException("generate " + role.toString() +
        " Certificate operation failed", ex, errorCode);

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
          scmCertificateServer.getCertificate(certSerialId);
      if (certificate != null) {
        return CertificateCodec.getPEMEncodedString(certificate);
      }
    } catch (CertificateException e) {
      throw new SCMSecurityException("getCertificate operation failed. ", e,
          GET_CERTIFICATE_FAILED);
    }
    LOGGER.info("Certificate with serial id {} not found.", certSerialId);
    throw new SCMSecurityException("Certificate not found",
        CERTIFICATE_NOT_FOUND);
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
          scmCertificateServer.getCACertificate());
    } catch (CertificateException e) {
      throw new SCMSecurityException("getRootCertificate operation failed. ",
          e, GET_CA_CERT_FAILED);
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
  public List<String> listCertificate(NodeType role,
      long startSerialId, int count, boolean isRevoked) throws IOException {
    List<X509Certificate> certificates =
        scmCertificateServer.listCertificate(role, startSerialId, count,
            isRevoked);
    List<String> results = new ArrayList<>(certificates.size());
    for (X509Certificate cert : certificates) {
      try {
        String certStr = CertificateCodec.getPEMEncodedString(cert);
        results.add(certStr);
      } catch (SCMSecurityException e) {
        throw new SCMSecurityException("listCertificate operation failed.",
            e, e.getErrorCode());
      }
    }
    return results;
  }

  @Override
  public List<String> listCACertificate() throws IOException {
    List<String> caCerts =
        listCertificate(NodeType.SCM, 0, 10, false);
    return caCerts;
  }

  @Override
  public String getRootCACertificate() throws IOException {
    LOGGER.debug("Getting Root CA certificate.");
    if (storageContainerManager.getScmStorageConfig()
        .checkPrimarySCMIdInitialized()) {
      return CertificateCodec.getPEMEncodedString(rootCACertificate);
    }
    return null;
  }

  @Override
  public List<CRLInfo> getCrls(List<Long> crlIds) throws IOException {
    return scmCertificateServer.getCrls(crlIds);
  }

  @Override
  public long getLatestCrlId() {
    return scmCertificateServer.getLatestCrlId();
  }

  @Override
  public long revokeCertificates(List<String> certIds, int reason,
      long revocationTime) throws IOException {
    storageContainerManager.checkAdminAccess(getRpcRemoteUser());

    Future<Optional<Long>> revoked = scmCertificateServer.revokeCertificates(
        certIds.stream().map(id -> new BigInteger(id))
            .collect(Collectors.toList()), CRLReason.lookup(reason),
        new Date(revocationTime));
    try {
      Long crlId = revoked.get().get();
      getGrpcUpdateServer().notifyCrlUpdate();
      return crlId;
    } catch (InterruptedException | ExecutionException e) {
      throw new SCMException("Fail to revoke certs",
          SCMException.ResultCodes.FAILED_TO_REVOKE_CERTIFICATES);
    }
  }

  public SCMUpdateServiceGrpcServer getGrpcUpdateServer() {
    return grpcUpdateServer;
  }

  @VisibleForTesting
  public UserGroupInformation getRpcRemoteUser() {
    return Server.getRemoteUser();
  }

  public RPC.Server getRpcServer() {
    return rpcServer;
  }

  public InetSocketAddress getRpcAddress() {
    return rpcAddress;
  }

  public void start() throws IOException {
    String startupMsg = StorageContainerManager.buildRpcServerStartMessage(
        "Starting RPC server for SCMSecurityProtocolServer.", getRpcAddress());
    LOGGER.info(startupMsg);
    metrics.register();
    getRpcServer().start();
    getGrpcUpdateServer().start();
  }

  public void stop() {
    try {
      LOGGER.info("Stopping the SCMSecurityProtocolServer.");
      metrics.unregister();
      getRpcServer().stop();
      getGrpcUpdateServer().stop();
    } catch (Exception ex) {
      LOGGER.error("SCMSecurityProtocolServer stop failed.", ex);
    }
  }

  public void join() throws InterruptedException {
    LOGGER.trace("Join RPC server for SCMSecurityProtocolServer.");
    getRpcServer().join();
    LOGGER.trace("Join gRPC server for SCMSecurityProtocolServer.");
    getGrpcUpdateServer().join();

  }

  public CertificateServer getRootCertificateServer() {
    return rootCertificateServer;
  }


  public CertificateServer getScmCertificateServer() {
    return scmCertificateServer;
  }
}

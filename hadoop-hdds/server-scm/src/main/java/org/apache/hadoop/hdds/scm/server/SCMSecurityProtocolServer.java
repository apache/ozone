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

import static org.apache.hadoop.hdds.scm.ScmUtils.checkIfCertSignRequestAllowed;
import static org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.CERTIFICATE_ID;
import static org.apache.hadoop.hdds.security.exception.SCMSecretKeyException.ErrorCode.SECRET_KEY_NOT_ENABLED;
import static org.apache.hadoop.hdds.security.exception.SCMSecretKeyException.ErrorCode.SECRET_KEY_NOT_INITIALIZED;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.CERTIFICATE_NOT_FOUND;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.GET_CA_CERT_FAILED;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.GET_CERTIFICATE_FAILED;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType.KERBEROS_TRUSTED;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec.getPEMEncodedString;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest.getCertificationRequest;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmNodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.protocolPB.SecretKeyProtocolDatanodePB;
import org.apache.hadoop.hdds.protocolPB.SecretKeyProtocolOmPB;
import org.apache.hadoop.hdds.protocolPB.SecretKeyProtocolScmPB;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.protocol.SCMSecurityProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocol.SecretKeyProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.security.exception.SCMSecretKeyException;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyManager;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The protocol used to perform security related operations with SCM.
 */
@KerberosInfo(
    serverPrincipal = ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public class SCMSecurityProtocolServer implements SCMSecurityProtocol,
    SecretKeyProtocolScm {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SCMSecurityProtocolServer.class);
  private CertificateServer rootCertificateServer;
  private final CertificateServer scmCertificateServer;
  private final RPC.Server rpcServer; // HADOOP RPC SERVER
  private final InetSocketAddress rpcAddress;
  private final ProtocolMessageMetrics metrics;
  private final ProtocolMessageMetrics secretKeyMetrics;
  private final StorageContainerManager storageContainerManager;
  private final CertificateClient scmCertificateClient;
  private final OzoneConfiguration config;
  private final SequenceIdGenerator sequenceIdGen;

  // SecretKey may not be enabled when neither block token nor container
  // token is enabled.
  private final SecretKeyManager secretKeyManager;

  SCMSecurityProtocolServer(OzoneConfiguration conf,
      @Nullable CertificateServer rootCertificateServer,
      CertificateServer scmCertificateServer,
      CertificateClient scmCertClient,
      StorageContainerManager scm,
      @Nullable SecretKeyManager secretKeyManager)
      throws IOException {
    this.storageContainerManager = scm;
    this.rootCertificateServer = rootCertificateServer;
    this.scmCertificateServer = scmCertificateServer;
    this.scmCertificateClient = scmCertClient;
    this.config = conf;
    this.sequenceIdGen = scm.getSequenceIdGen();
    this.secretKeyManager = secretKeyManager;
    final int handlerCount =
        conf.getInt(ScmConfigKeys.OZONE_SCM_SECURITY_HANDLER_COUNT_KEY,
            ScmConfigKeys.OZONE_SCM_SECURITY_HANDLER_COUNT_DEFAULT);
    final int readThreads = conf.getInt(ScmConfigKeys.OZONE_SCM_SECURITY_READ_THREADPOOL_KEY,
        ScmConfigKeys.OZONE_SCM_SECURITY_READ_THREADPOOL_DEFAULT);
    rpcAddress = HddsServerUtil
        .getScmSecurityInetAddress(conf);
    // SCM security service RPC service.
    RPC.setProtocolEngine(conf, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);
    metrics = new ProtocolMessageMetrics("ScmSecurityProtocol",
        "SCM Security protocol metrics",
        SCMSecurityProtocolProtos.Type.class);
    secretKeyMetrics = new ProtocolMessageMetrics("ScmSecretKeyProtocol",
        "SCM SecretKey protocol metrics",
        SCMSecretKeyProtocolProtos.Type.class);
    BlockingService secureProtoPbService =
        SCMSecurityProtocolProtos.SCMSecurityProtocolService
            .newReflectiveBlockingService(
                new SCMSecurityProtocolServerSideTranslatorPB(this,
                    scm, metrics));
    BlockingService secretKeyService =
        SCMSecretKeyProtocolProtos.SCMSecretKeyProtocolService
            .newReflectiveBlockingService(
                new SecretKeyProtocolServerSideTranslatorPB(
                    this, scm, secretKeyMetrics)
        );
    this.rpcServer =
        StorageContainerManager.startRpcServer(
            conf,
            rpcAddress,
            SCMSecurityProtocolPB.class,
            secureProtoPbService,
            handlerCount,
            readThreads);
    HddsServerUtil.addPBProtocol(conf, SecretKeyProtocolDatanodePB.class,
        secretKeyService, rpcServer);
    HddsServerUtil.addPBProtocol(conf, SecretKeyProtocolOmPB.class,
        secretKeyService, rpcServer);
    HddsServerUtil.addPBProtocol(conf, SecretKeyProtocolScmPB.class,
        secretKeyService, rpcServer);
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
      DatanodeDetailsProto dnDetails, String certSignReq) throws IOException {
    LOGGER.info("Processing CSR for dn {}, UUID: {}", dnDetails.getHostName(),
        dnDetails.getUuid());
    Objects.requireNonNull(dnDetails);

    checkIfCertSignRequestAllowed(
        storageContainerManager.getRootCARotationManager(), false, config,
        "getDataNodeCertificate");

    return getEncodedCertToString(certSignReq, NodeType.DATANODE);
  }

  @Override
  public String getCertificate(
      NodeDetailsProto nodeDetails,
      String certSignReq) throws IOException {
    LOGGER.info("Processing CSR for {} {}, UUID: {}",
        nodeDetails.getNodeType(), nodeDetails.getHostName(),
        nodeDetails.getUuid());
    Objects.requireNonNull(nodeDetails);

    checkIfCertSignRequestAllowed(
        storageContainerManager.getRootCARotationManager(), false, config,
        "getCertificate");

    return getEncodedCertToString(certSignReq, nodeDetails.getNodeType());
  }

  @Override
  public ManagedSecretKey getCurrentSecretKey() throws SCMSecretKeyException {
    validateSecretKeyStatus();
    return secretKeyManager.getCurrentSecretKey();
  }

  @Override
  public ManagedSecretKey getSecretKey(UUID id) throws SCMSecretKeyException {
    validateSecretKeyStatus();
    return secretKeyManager.getSecretKey(id);
  }

  @Override
  public List<ManagedSecretKey> getAllSecretKeys()
      throws SCMSecretKeyException {
    validateSecretKeyStatus();
    return secretKeyManager.getSortedKeys();
  }

  private void validateSecretKeyStatus() throws SCMSecretKeyException {
    if (secretKeyManager == null) {
      throw new SCMSecretKeyException("Secret keys are not enabled.",
          SECRET_KEY_NOT_ENABLED);
    }

    if (!secretKeyManager.isInitialized()) {
      throw new SCMSecretKeyException(
          "Secret key initialization is not finished yet.",
          SECRET_KEY_NOT_INITIALIZED);
    }
  }

  @Override
  public boolean checkAndRotate(boolean force) throws SCMSecretKeyException {
    validateSecretKeyStatus();
    try {
      return secretKeyManager.checkAndRotate(force);
    } catch (SCMException ex) {
      LOGGER.error("Error rotating secret keys", ex);
      throw new SCMSecretKeyException(ex.getMessage(),
          SCMSecretKeyException.ErrorCode.INTERNAL_ERROR);
    }
  }

  @Override
  public synchronized List<String> getAllRootCaCertificates()
      throws IOException {
    List<String> pemEncodedList = new ArrayList<>();
    Set<X509Certificate> certList =
        scmCertificateClient.getAllRootCaCerts().isEmpty() ?
            scmCertificateClient.getAllCaCerts() :
            scmCertificateClient.getAllRootCaCerts();
    for (X509Certificate cert : certList) {
      pemEncodedList.add(getPEMEncodedString(cert));
    }
    return pemEncodedList;
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

    checkIfCertSignRequestAllowed(
        storageContainerManager.getRootCARotationManager(), false, config,
        "getOMCertificate");

    return getEncodedCertToString(certSignReq, NodeType.OM);
  }

  /**
   * Get signed certificate for SCM Node.
   *
   * @param scmNodeDetails   - SCM Node Details.
   * @param certSignReq      - Certificate signing request.
   * @return String          - SCM signed pem encoded certificate.
   */
  @Override
  public String getSCMCertificate(ScmNodeDetailsProto scmNodeDetails,
      String certSignReq) throws IOException {
    return getSCMCertificate(scmNodeDetails, certSignReq, false);
  }

  /**
   * Get signed certificate for SCM Node.
   *
   * @param scmNodeDetails   - SCM Node Details.
   * @param certSignReq      - Certificate signing request.
   * @param isRenew          - if SCM is renewing certificate or not.
   * @return String          - SCM signed pem encoded certificate.
   */
  @Override
  public String getSCMCertificate(ScmNodeDetailsProto scmNodeDetails,
      String certSignReq, boolean isRenew) throws IOException {
    Objects.requireNonNull(scmNodeDetails);
    // Check clusterID
    if (!storageContainerManager.getClusterId().equals(
        scmNodeDetails.getClusterId())) {
      throw new IOException("SCM ClusterId mismatch. Peer SCM ClusterId " +
          scmNodeDetails.getClusterId() + ", primary SCM ClusterId "
          + storageContainerManager.getClusterId());
    }

    checkIfCertSignRequestAllowed(
        storageContainerManager.getRootCARotationManager(), isRenew, config,
        "getSCMCertificate");

    LOGGER.info("Processing CSR for scm {}, nodeId: {}",
        scmNodeDetails.getHostName(), scmNodeDetails.getScmNodeId());

    return getEncodedCertToString(certSignReq, NodeType.SCM);
  }

  /**
   *  Request certificate for the specified role.
   * @param certSignReq - Certificate signing request.
   * @param nodeType - role OM/SCM/DATANODE
   * @return String         - SCM signed pem encoded certificate.
   * @throws IOException
   */
  private synchronized String getEncodedCertToString(String certSignReq,
      NodeType nodeType) throws IOException {
    Future<CertPath> future;
    PKCS10CertificationRequest csr = getCertificationRequest(certSignReq);
    if (nodeType == NodeType.SCM && rootCertificateServer != null) {
      future = rootCertificateServer.requestCertificate(csr,
          KERBEROS_TRUSTED, nodeType, getNextCertificateId());
    } else {
      future = scmCertificateServer.requestCertificate(csr,
          KERBEROS_TRUSTED, nodeType, getNextCertificateId());
    }
    try {
      return getPEMEncodedString(future.get());
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
    } else if (role == NodeType.DATANODE) {
      errorCode = SCMSecurityException.ErrorCode.GET_DN_CERTIFICATE_FAILED;
    } else {
      errorCode = SCMSecurityException.ErrorCode.GET_CERTIFICATE_FAILED;
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
        return getPEMEncodedString(certificate);
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
      return getPEMEncodedString(
          scmCertificateServer.getCaCertPath());
    } catch (CertificateException e) {
      throw new SCMSecurityException("getRootCertificate operation failed. ",
          e, GET_CA_CERT_FAILED);
    }
  }

  /**
   * @param role          - node role: OM/SCM/DN.
   * @param startSerialId - start certificate serial id.
   * @param count         - max number of certificates returned in a batch.
   * @throws IOException
   */
  @Override
  public List<String> listCertificate(NodeType role,
      long startSerialId, int count) throws IOException {
    List<X509Certificate> certificates = scmCertificateServer.listCertificate(role, startSerialId, count);
    List<String> results = new ArrayList<>(certificates.size());
    for (X509Certificate cert : certificates) {
      try {
        String certStr = getPEMEncodedString(cert);
        results.add(certStr);
      } catch (SCMSecurityException e) {
        throw new SCMSecurityException("listCertificate operation failed.", e, e.getErrorCode());
      }
    }
    return results;
  }

  @Override
  public List<String> listCACertificate() throws IOException {
    List<String> caCerts =
        listCertificate(NodeType.SCM, 0, 10);
    return caCerts;
  }

  @Override
  public synchronized String getRootCACertificate() throws IOException {
    LOGGER.debug("Getting Root CA certificate.");
    if (rootCertificateServer != null) {
      try {
        return CertificateCodec.getPEMEncodedString(
            rootCertificateServer.getCACertificate());
      } catch (CertificateException e) {
        LOGGER.error("Failed to get root CA certificate", e);
        throw new IOException("Failed to get root CA certificate", e);
      }
    }

    return CertificateCodec.getPEMEncodedString(
        scmCertificateClient.getCACertificate());
  }

  @Override
  public List<String> removeExpiredCertificates() throws IOException {
    storageContainerManager.checkAdminAccess(getRpcRemoteUser(), false);
    List<String> pemEncodedCerts = new ArrayList<>();
    for (X509Certificate cert : storageContainerManager.getCertificateStore()
        .removeAllExpiredCertificates()) {
      pemEncodedCerts.add(CertificateCodec.getPEMEncodedString(cert));
    }
    return pemEncodedCerts;
  }

  private String getNextCertificateId() throws IOException {
    return String.valueOf(sequenceIdGen.getNextId(CERTIFICATE_ID));
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
    LOGGER.info("Join RPC server for SCMSecurityProtocolServer.");
    getRpcServer().join();
    LOGGER.info("Join gRPC server for SCMSecurityProtocolServer.");
  }

  public synchronized CertificateServer getRootCertificateServer() {
    return rootCertificateServer;
  }

  public synchronized void setRootCertificateServer(
      CertificateServer newServer) {
    this.rootCertificateServer = newServer;
  }

  public CertificateServer getScmCertificateServer() {
    return scmCertificateServer;
  }
}

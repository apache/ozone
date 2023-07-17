/*
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmNodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.proxy.SCMSecurityProtocolFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.ssl.KeyStoresFactory;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCAServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.PKIProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.TimeDuration;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_INFO_WAIT_DURATION;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_INFO_WAIT_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType.KERBEROS_TRUSTED;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest.getEncodedString;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClientWithMaxRetry;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_SUB_CA_PREFIX;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;

/**
 * Utilities for SCM HA security.
 */
public final class HASecurityUtils {

  private HASecurityUtils() {
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(HASecurityUtils.class);

  /**
   * Initialize Security which generates public, private key pair and get SCM
   * signed certificate and persist to local disk.
   * @param scmStorageConfig
   * @param conf
   * @param scmHostname
   * @throws IOException
   */
  public static void initializeSecurity(SCMStorageConfig scmStorageConfig,
      OzoneConfiguration conf, String scmHostname, boolean primaryscm)
      throws IOException {
    LOG.info("Initializing secure StorageContainerManager.");

    SecurityConfig securityConfig = new SecurityConfig(conf);
    SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
        getScmSecurityClientWithMaxRetry(conf, getCurrentUser());
    try (CertificateClient certClient =
        new SCMCertificateClient(securityConfig, scmSecurityClient,
            scmStorageConfig.getScmId(), scmStorageConfig.getClusterID(),
            scmStorageConfig.getScmCertSerialId(), scmHostname)) {
      InitResponse response = certClient.init();
      LOG.info("Init response: {}", response);
      switch (response) {
      case SUCCESS:
        LOG.info("Initialization successful.");
        break;
      case GETCERT:
        if (!primaryscm) {
          getRootCASignedSCMCert(conf, certClient, securityConfig,
              scmStorageConfig, scmHostname);
        } else {
          getPrimarySCMSelfSignedCert(certClient, securityConfig,
              scmStorageConfig, scmHostname);
        }
        LOG.info("Successfully stored SCM signed certificate.");
        break;
      case FAILURE:
        LOG.error("SCM security initialization failed.");
        throw new RuntimeException("OM security initialization failed.");
      case RECOVER:
        LOG.error("SCM security initialization failed. SCM certificate is " +
            "missing.");
        throw new RuntimeException("SCM security initialization failed.");
      default:
        LOG.error("SCM security initialization failed. Init response: {}",
            response);
        throw new RuntimeException("SCM security initialization failed.");
      }
    }
  }

  /**
   * For bootstrapped SCM get sub-ca signed certificate and root CA
   * certificate using scm security client and store it using certificate
   * client.
   */
  private static void getRootCASignedSCMCert(
      OzoneConfiguration configuration, CertificateClient client,
      SecurityConfig securityConfig,
      SCMStorageConfig scmStorageConfig, String scmHostname) {
    try {
      // Generate CSR.
      PKCS10CertificationRequest csr = generateCSR(client, scmStorageConfig,
          securityConfig, scmHostname);

      ScmNodeDetailsProto scmNodeDetailsProto =
          ScmNodeDetailsProto.newBuilder()
              .setClusterId(scmStorageConfig.getClusterID())
              .setHostName(scmHostname)
              .setScmNodeId(scmStorageConfig.getScmId()).build();

      // Create SCM security client.
      SCMSecurityProtocolClientSideTranslatorPB secureScmClient =
          getScmSecurityClientWithFixedDuration(configuration);

      // Get SCM sub CA cert.
      SCMGetCertResponseProto response = secureScmClient.
          getSCMCertChain(scmNodeDetailsProto, getEncodedString(csr), false);
      String pemEncodedCert = response.getX509Certificate();

      // Store SCM sub CA and root CA certificate.
      if (response.hasX509CACertificate()) {
        String pemEncodedRootCert = response.getX509CACertificate();
        client.storeCertificate(
            pemEncodedRootCert, CAType.SUBORDINATE);
        client.storeCertificate(pemEncodedCert, CAType.NONE);
        //note: this does exactly the same as store certificate
        persistSubCACertificate(securityConfig, client,
            pemEncodedCert);

        X509Certificate certificate =
            CertificateCodec.getX509Certificate(pemEncodedCert);
        // Persist scm cert serial ID.
        scmStorageConfig.setScmCertSerialId(certificate.getSerialNumber()
            .toString());
      } else {
        throw new RuntimeException("Unable to retrieve SCM certificate chain");
      }
    } catch (IOException | CertificateException e) {
      LOG.error("Error while fetching/storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }
  }


  /**
   * For primary SCM get sub-ca signed certificate and root CA certificate by
   * root CA certificate server and store it using certificate client.
   */
  private static void getPrimarySCMSelfSignedCert(CertificateClient client,
      SecurityConfig config, SCMStorageConfig scmStorageConfig,
      String scmHostname) {

    try {

      CertificateServer rootCAServer =
          initializeRootCertificateServer(config, null, scmStorageConfig,
              new DefaultCAProfile());

      PKCS10CertificationRequest csr = generateCSR(client, scmStorageConfig,
          config, scmHostname);

      CertPath subSCMCertHolderList = rootCAServer.
          requestCertificate(csr, KERBEROS_TRUSTED, SCM).get();

      CertPath rootCACertificatePath =
          rootCAServer.getCaCertPath();

      String pemEncodedCert =
          CertificateCodec.getPEMEncodedString(subSCMCertHolderList);

      String pemEncodedRootCert =
          CertificateCodec.getPEMEncodedString(rootCACertificatePath);

      client.storeCertificate(
          pemEncodedRootCert, CAType.SUBORDINATE);
      client.storeCertificate(pemEncodedCert, CAType.NONE);
      //note: this does exactly the same as store certificate
      persistSubCACertificate(config, client, pemEncodedCert);
      X509Certificate cert =
          (X509Certificate) subSCMCertHolderList.getCertificates().get(0);
      X509CertificateHolder subSCMCertHolder =
          CertificateCodec.getCertificateHolder(cert);

      // Persist scm cert serial ID.
      scmStorageConfig.setScmCertSerialId(subSCMCertHolder.getSerialNumber()
          .toString());
    } catch (InterruptedException | ExecutionException | IOException |
        CertificateException  e) {
      LOG.error("Error while fetching/storing SCM signed certificate.", e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

  }

  /**
   * This function creates/initializes a certificate server as needed.
   * This function is idempotent, so calling this again and again after the
   * server is initialized is not a problem.
   *
   * @param config
   * @param scmCertStore
   * @param scmStorageConfig
   * @param pkiProfile
   * @param component
   */
  public static CertificateServer initializeRootCertificateServer(
      SecurityConfig config, CertificateStore scmCertStore,
      SCMStorageConfig scmStorageConfig, BigInteger rootCertId,
      PKIProfile pkiProfile, String component) throws IOException {
    String subject = String.format(SCM_ROOT_CA_PREFIX, rootCertId) +
        InetAddress.getLocalHost().getHostName();

    DefaultCAServer rootCAServer = new DefaultCAServer(subject,
        scmStorageConfig.getClusterID(),
        scmStorageConfig.getScmId(), scmCertStore, rootCertId, pkiProfile,
        component);

    rootCAServer.init(config, CAType.ROOT);

    return rootCAServer;
  }

  /**
   * This function creates/initializes a certificate server as needed.
   * This function is idempotent, so calling this again and again after the
   * server is initialized is not a problem.
   *
   * @param config
   * @param scmCertStore
   * @param scmStorageConfig
   * @param pkiProfile
   */
  public static CertificateServer initializeRootCertificateServer(
      SecurityConfig config, CertificateStore scmCertStore,
      SCMStorageConfig scmStorageConfig, PKIProfile pkiProfile)
      throws IOException {
    return initializeRootCertificateServer(config, scmCertStore,
        scmStorageConfig, BigInteger.ONE, pkiProfile,
        SCM_ROOT_CA_COMPONENT_NAME);
  }

  /**
   * Generate CSR to obtain SCM sub CA certificate.
   */
  private static PKCS10CertificationRequest generateCSR(
      CertificateClient client, SCMStorageConfig scmStorageConfig,
      SecurityConfig config, String scmHostname)
      throws IOException {
    CertificateSignRequest.Builder builder = client.getCSRBuilder();

    // Get host name.
    String subject = String.format(SCM_SUB_CA_PREFIX, System.nanoTime())
        + scmHostname;

    builder.setConfiguration(config)
        .setScmID(scmStorageConfig.getScmId())
        .setClusterID(scmStorageConfig.getClusterID())
        .setSubject(subject);

    LOG.info("Creating csr for SCM->hostName:{},scmId:{},clusterId:{}," +
            "subject:{}", scmHostname, scmStorageConfig.getScmId(),
        scmStorageConfig.getClusterID(), subject);

    return builder.build();
  }

  /**
   * Persists the sub SCM signed certificate to the location which can be
   * read by sub CA Certificate server.
   *
   * @param config
   * @param certificateClient
   * @param certificateHolder
   * @throws IOException
   */
  private static void persistSubCACertificate(SecurityConfig config,
      CertificateClient certificateClient,
      String certificateHolder) throws IOException {
    CertificateCodec certCodec =
        new CertificateCodec(config, certificateClient.getComponentName());

    certCodec.writeCertificate(certCodec.getLocation().toAbsolutePath(),
        config.getCertificateFileName(), certificateHolder);
  }

  /**
   * Create GrpcTlsConfig.
   *
   * @param conf
   * @param certificateClient
   * @return
   */
  public static GrpcTlsConfig createSCMRatisTLSConfig(SecurityConfig conf,
      CertificateClient certificateClient) throws IOException {
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      KeyStoresFactory serverKeyFactory =
          certificateClient.getServerKeyStoresFactory();

      return new GrpcTlsConfig(serverKeyFactory.getKeyManagers()[0],
          serverKeyFactory.getTrustManagers()[0], true);
    }
    return null;
  }

  /**
   * Submit SCM request to ratis using RaftClient.
   * @param raftGroup
   * @param tlsConfig
   * @param message
   * @return SCMRatisResponse.
   * @throws Exception
   */
  public static SCMRatisResponse submitScmRequestToRatis(RaftGroup raftGroup,
      GrpcTlsConfig tlsConfig, Message message) throws Exception {

    // TODO: GRPC TLS only for now, netty/hadoop RPC TLS support later.
    final SupportedRpcType rpc = SupportedRpcType.GRPC;
    final RaftProperties properties = RatisHelper.newRaftProperties(rpc);

    // For now not making anything configurable, RaftClient  is only used
    // in SCM for DB updates of sub-ca certs go via Ratis.
    RaftClient.Builder builder = RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setLeaderId(null)
        .setProperties(properties)
        .setParameters(RatisHelper.setClientTlsConf(rpc, tlsConfig))
        .setRetryPolicy(
            RetryPolicies.retryUpToMaximumCountWithFixedSleep(120,
                TimeDuration.valueOf(500, TimeUnit.MILLISECONDS)));
    try (RaftClient raftClient = builder.build()) {
      CompletableFuture<RaftClientReply> future =
          raftClient.async().send(message);
      RaftClientReply raftClientReply = future.get();
      return SCMRatisResponse.decode(raftClientReply);
    }
  }

  private static SCMSecurityProtocolClientSideTranslatorPB
      getScmSecurityClientWithFixedDuration(OzoneConfiguration conf)
      throws IOException {
    // As for OM during init, we need to wait for specific duration so that
    // we can give response to user performed operation init in a definite
    // period, instead of stuck for ever.
    long duration = conf.getTimeDuration(OZONE_SCM_INFO_WAIT_DURATION,
        OZONE_SCM_INFO_WAIT_DURATION_DEFAULT, TimeUnit.SECONDS);
    SCMClientConfig scmClientConfig = conf.getObject(SCMClientConfig.class);
    int retryCount =
        (int) (duration / (scmClientConfig.getRetryInterval() / 1000));

    // If duration is set to lesser value, fall back to actual default
    // retry count.
    if (retryCount > scmClientConfig.getRetryCount()) {
      scmClientConfig.setRetryCount(retryCount);
      conf.setFromObject(scmClientConfig);
    }

    return new SCMSecurityProtocolClientSideTranslatorPB(
        new SCMSecurityProtocolFailoverProxyProvider(conf,
            UserGroupInformation.getCurrentUser()));

  }

  public static boolean isSelfSignedCertificate(X509Certificate cert) {
    return cert.getIssuerX500Principal().equals(cert.getSubjectX500Principal());
  }

  public static boolean isCACertificate(X509Certificate cert) {
    return cert.getBasicConstraints() != -1;
  }
}

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
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCAServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.PKIProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.util.TimeDuration;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType.KERBEROS_TRUSTED;
import static org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest.getEncodedString;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_SUB_CA_PREFIX;

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
   * @param scmAddress
   * @throws IOException
   */
  public static void initializeSecurity(SCMStorageConfig scmStorageConfig,
      OzoneConfiguration conf,
      InetSocketAddress scmAddress, boolean primaryscm)
      throws IOException {
    LOG.info("Initializing secure StorageContainerManager.");

    CertificateClient certClient =
        new SCMCertificateClient(new SecurityConfig(conf));
    InitResponse response = certClient.init();
    LOG.info("Init response: {}", response);
    switch (response) {
    case SUCCESS:
      LOG.info("Initialization successful.");
      break;
    case GETCERT:
      if (!primaryscm) {
        getRootCASignedSCMCert(certClient, conf, scmStorageConfig,
            scmAddress);
      } else {
        getPrimarySCMSelfSignedCert(certClient, conf, scmStorageConfig,
            scmAddress);
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

  /**
   * For bootstrapped SCM get sub-ca signed certificate and root CA
   * certificate using scm security client and store it using certificate
   * client.
   */
  private static void getRootCASignedSCMCert(CertificateClient client,
      OzoneConfiguration config,
      SCMStorageConfig scmStorageConfig, InetSocketAddress scmAddress) {
    try {
      // Generate CSR.
      PKCS10CertificationRequest csr = generateCSR(client, scmStorageConfig,
          config, scmAddress);

      ScmNodeDetailsProto scmNodeDetailsProto =
          ScmNodeDetailsProto.newBuilder()
              .setClusterId(scmStorageConfig.getClusterID())
              .setHostName(scmAddress.getHostName())
              .setScmNodeId(scmStorageConfig.getScmId()).build();

      // Create SCM security client.
      SCMSecurityProtocolClientSideTranslatorPB secureScmClient =
          HddsServerUtil.getScmSecurityClientWithFixedDuration(config);

      // Get SCM sub CA cert.
      SCMGetCertResponseProto response = secureScmClient.
          getSCMCertChain(scmNodeDetailsProto, getEncodedString(csr));
      String pemEncodedCert = response.getX509Certificate();

      // Store SCM sub CA and root CA certificate.
      if (response.hasX509CACertificate()) {
        String pemEncodedRootCert = response.getX509CACertificate();
        client.storeCertificate(pemEncodedRootCert, true, true);
        client.storeCertificate(pemEncodedCert, true);

        X509Certificate certificate =
            CertificateCodec.getX509Certificate(pemEncodedCert);

        persistSubCACertificate(config, client,
            CertificateCodec.getCertificateHolder(certificate));

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
      OzoneConfiguration config, SCMStorageConfig scmStorageConfig,
      InetSocketAddress scmAddress) {

    try {

      CertificateServer rootCAServer =
          initializeRootCertificateServer(config, null, scmStorageConfig,
              new DefaultCAProfile());

      PKCS10CertificationRequest csr = generateCSR(client, scmStorageConfig,
          config, scmAddress);

      X509CertificateHolder subSCMCertHolder = rootCAServer.
          requestCertificate(csr, KERBEROS_TRUSTED, SCM).get();

      X509CertificateHolder rootCACertificateHolder =
          rootCAServer.getCACertificate();

      String pemEncodedCert =
          CertificateCodec.getPEMEncodedString(subSCMCertHolder);

      String pemEncodedRootCert =
          CertificateCodec.getPEMEncodedString(rootCACertificateHolder);


      client.storeCertificate(pemEncodedRootCert, true, true);
      client.storeCertificate(pemEncodedCert, true);


      persistSubCACertificate(config, client, subSCMCertHolder);

      // Persist scm cert serial ID.
      scmStorageConfig.setScmCertSerialId(subSCMCertHolder.getSerialNumber()
          .toString());
    } catch (InterruptedException | ExecutionException| IOException |
        CertificateException  e) {
      LOG.error("Error while fetching/storing SCM signed certificate.", e);
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
   */
  public static CertificateServer initializeRootCertificateServer(
      OzoneConfiguration config, CertificateStore scmCertStore,
      SCMStorageConfig scmStorageConfig, PKIProfile pkiProfile)
      throws IOException {
    String subject = SCM_ROOT_CA_PREFIX +
        InetAddress.getLocalHost().getHostName();

    DefaultCAServer rootCAServer = new DefaultCAServer(subject,
        scmStorageConfig.getClusterID(),
        scmStorageConfig.getScmId(), scmCertStore, pkiProfile,
        SCM_ROOT_CA_COMPONENT_NAME);

    rootCAServer.init(new SecurityConfig(config),
        CertificateServer.CAType.SELF_SIGNED_CA);

    return rootCAServer;
  }

  /**
   * Generate CSR to obtain SCM sub CA certificate.
   */
  private static PKCS10CertificationRequest generateCSR(
      CertificateClient client, SCMStorageConfig scmStorageConfig,
      OzoneConfiguration config, InetSocketAddress scmAddress)
      throws IOException {
    CertificateSignRequest.Builder builder = client.getCSRBuilder();
    KeyPair keyPair = new KeyPair(client.getPublicKey(),
        client.getPrivateKey());

    // Get host name.
    String hostname = scmAddress.getAddress().getHostName();

    String subject = SCM_SUB_CA_PREFIX + hostname;

    builder.setKey(keyPair)
        .setConfiguration(config)
        .setScmID(scmStorageConfig.getScmId())
        .setClusterID(scmStorageConfig.getClusterID())
        .setSubject(subject);


    LOG.info("Creating csr for SCM->hostName:{},scmId:{},clusterId:{}," +
            "subject:{}", hostname, scmStorageConfig.getScmId(),
        scmStorageConfig.getClusterID(), subject);

    return builder.build();
  }

  /**
   * Persists the sub SCM signed certificate to the location which can be
   * read by sub CA Certificate server.
   * @param config
   * @param certificateClient
   * @param certificateHolder
   * @throws IOException
   */
  private static void persistSubCACertificate(OzoneConfiguration config,
      CertificateClient certificateClient,
      X509CertificateHolder certificateHolder) throws IOException {
    CertificateCodec certCodec =
        new CertificateCodec(new SecurityConfig(config),
            certificateClient.getComponentName());

    certCodec.writeCertificate(certificateHolder);
  }

  /**
   * Create Server TLS parameters required for Ratis Server.

   * @return Parameter map set with TLS config.
   */
  public static Parameters createSCMServerTlsParameters(
      GrpcTlsConfig grpcTlsConfig) {
    Parameters parameters = new Parameters();

    if (grpcTlsConfig != null) {
      GrpcConfigKeys.Server.setTlsConf(parameters, grpcTlsConfig);
      GrpcConfigKeys.Admin.setTlsConf(parameters, grpcTlsConfig);
      GrpcConfigKeys.Client.setTlsConf(parameters, grpcTlsConfig);
      GrpcConfigKeys.TLS.setConf(parameters, grpcTlsConfig);
    }

    return parameters;
  }

  /**
   * Create GrpcTlsConfig.
   * @param conf
   * @param certificateClient
   * @return
   */
  public static GrpcTlsConfig createSCMRatisTLSConfig(SecurityConfig conf,
      CertificateClient certificateClient) {
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      return new GrpcTlsConfig(
          certificateClient.getPrivateKey(), certificateClient.getCertificate(),
          certificateClient.getCACertificate(), true);
    }
    return null;
  }

  /**
   * Submit SCM certs request to ratis using RaftClient.
   * @param raftGroup
   * @param tlsConfig
   * @param message
   * @return SCMRatisResponse.
   * @throws Exception
   */
  public static SCMRatisResponse submitScmCertsToRatis(RaftGroup raftGroup,
      GrpcTlsConfig tlsConfig, Message message) throws Exception {
    final RaftProperties properties = new RaftProperties();

    // TODO: GRPC TLS only for now, netty/hadoop RPC TLS support later.
    RaftConfigKeys.Rpc.setType(properties, RpcType.valueOf("GRPC"));


    // For now not making anything configurable, RaftClient  is only used
    // in SCM for DB updates of sub-ca certs go via Ratis.
    RaftClient.Builder builder =  RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setLeaderId(null)
        .setProperties(properties)
        .setRetryPolicy(
            RetryPolicies.retryUpToMaximumCountWithFixedSleep(15,
                TimeDuration.valueOf(500, TimeUnit.MILLISECONDS)));

    if (tlsConfig != null) {
      Parameters parameters = new Parameters();
      GrpcConfigKeys.Client.setTlsConf(parameters, tlsConfig);
      builder.setParameters(parameters);
    }

    RaftClient raftClient =  builder.build();

    CompletableFuture<RaftClientReply> future =
        raftClient.async().send(message);

    RaftClientReply raftClientReply = future.get();

    return SCMRatisResponse.decode(raftClientReply);

  }


}

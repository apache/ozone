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

package org.apache.hadoop.hdds.scm.ha;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_INFO_WAIT_DURATION;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_INFO_WAIT_DURATION_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_PREFIX;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.proxy.SCMSecurityProtocolFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCAServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.PKIProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for SCM HA security.
 */
public final class  HASecurityUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HASecurityUtils.class);

  private HASecurityUtils() {
  }

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
        getScmSecurityClientWithFixedDuration(conf);
    try (CertificateClient certClient =
        new SCMCertificateClient(securityConfig, scmSecurityClient,
            scmStorageConfig.getScmId(), scmStorageConfig.getClusterID(),
            scmStorageConfig.getScmCertSerialId(), scmHostname, primaryscm,
            certIDString -> {
              try {
                scmStorageConfig.setScmCertSerialId(certIDString);
              } catch (IOException e) {
                LOG.error("Failed to set new certificate ID", e);
                throw new RuntimeException("Failed to set new certificate ID");
              }
            })) {
      certClient.initWithRecovery();
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
    String subject = SCM_ROOT_CA_PREFIX +
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
   * Create GrpcTlsConfig.
   *
   * @param conf
   * @param certificateClient
   */
  public static GrpcTlsConfig createSCMRatisTLSConfig(SecurityConfig conf,
      CertificateClient certificateClient) throws IOException {
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      return new GrpcTlsConfig(certificateClient.getKeyManager(),
          certificateClient.getTrustManager(), true);
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

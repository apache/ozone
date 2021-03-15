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
import org.apache.hadoop.hdds.scm.server.SCMCertStore;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCAServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.PKIProfiles.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType.KERBEROS_TRUSTED;
import static org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest.getEncodedString;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_CERT_STORAGE_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_PATH;

public final class HASecurityUtils {

  private HASecurityUtils() {
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(HASecurityUtils.class);

  /**
   * Initialize Security which generates public, private key pair and get SCM
   * signed certificate and persist to local disk.
   * @param scmStorageConfig
   * @param fetchedScmId
   * @param conf
   * @param scmAddress
   * @throws IOException
   */
  public static void initializeSecurity(SCMStorageConfig scmStorageConfig,
      String fetchedScmId, OzoneConfiguration conf,
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
        getRootCASignedSCMCert(certClient, conf, fetchedScmId, scmStorageConfig,
            scmAddress);
      } else {
        getPrimarySCMSelfSignedCert(certClient, conf, fetchedScmId,
            scmStorageConfig, scmAddress);
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
   * Get SCM signed certificate and store it using certificate client.
   */
  private static void getRootCASignedSCMCert(CertificateClient client,
      OzoneConfiguration config, String fetchedSCMId,
      SCMStorageConfig scmStorageConfig, InetSocketAddress scmAddress)
      throws IOException {

    PKCS10CertificationRequest csr = generateCSR(client, scmStorageConfig,
        config, scmAddress, fetchedSCMId);

    ScmNodeDetailsProto scmNodeDetailsProto =
        ScmNodeDetailsProto.newBuilder()
            .setClusterId(scmStorageConfig.getClusterID())
            .setHostName(scmAddress.getHostName())
            .setScmNodeId(fetchedSCMId).build();

    SCMSecurityProtocolClientSideTranslatorPB secureScmClient =
        HddsServerUtil.getScmSecurityClient(config);

    SCMGetCertResponseProto response = secureScmClient.
        getSCMCertChain(scmNodeDetailsProto, getEncodedString(csr));
    String pemEncodedCert = response.getX509Certificate();

    try {

      // Store SCM CA certificate.
      if (response.hasX509CACertificate()) {
        String pemEncodedRootCert = response.getX509CACertificate();
        client.storeCertificate(pemEncodedRootCert, true, true);
        client.storeCertificate(pemEncodedCert, true);


        // Write to the location, so that Default CA server can read the
        // certificate generated by cert client.
        CertificateCodec certCodec =
            new CertificateCodec(new SecurityConfig(config),
                client.getComponentName());

        X509Certificate certificate =
            CertificateCodec.getX509Certificate(pemEncodedCert);
        certCodec.writeCertificate(certCodec.getCertificateHolder(certificate));

        // Persist scm cert serial ID.
        scmStorageConfig.setScmCertSerialId(certificate.getSerialNumber()
            .toString());
      } else {
        throw new RuntimeException("Unable to retrieve SCM certificate chain");
      }
    } catch (IOException | CertificateException e) {
      LOG.error("Error while storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }
  }

  private static void getPrimarySCMSelfSignedCert(CertificateClient client,
      OzoneConfiguration config, String fetchedSCMId,
      SCMStorageConfig scmStorageConfig, InetSocketAddress scmAddress)
      throws IOException {

    PKCS10CertificationRequest csr = generateCSR(client, scmStorageConfig,
        config, scmAddress, fetchedSCMId);

    CertificateServer rootCAServer =
        initializeRootCertificateServer(scmStorageConfig.getClusterID(),
            scmStorageConfig.getScmId(), null);

    rootCAServer.init(new SecurityConfig(config),
        CertificateServer.CAType.SELF_SIGNED_CA);

    try {
      X509CertificateHolder certificateHolder = rootCAServer.
          requestCertificate(csr, KERBEROS_TRUSTED, SCM).get();

      X509CertificateHolder rootCAServerCACertificateHolder =
          rootCAServer.getCACertificate();

      String pemEncodedCert =
          CertificateCodec.getPEMEncodedString(certificateHolder);

      String pemEncodedRootCert =
          CertificateCodec.getPEMEncodedString(rootCAServerCACertificateHolder);


      client.storeCertificate(pemEncodedRootCert, true, true);
      client.storeCertificate(pemEncodedCert, true);

      // Write to the location, so that Default CA server can read the
      // certificate generated by cert client.
      CertificateCodec certCodec =
          new CertificateCodec(new SecurityConfig(config),
              client.getComponentName());

      certCodec.writeCertificate(certificateHolder);

      // Persist scm cert serial ID.
      scmStorageConfig.setScmCertSerialId(certificateHolder.getSerialNumber()
          .toString());
    } catch (InterruptedException e) {
      LOG.error("Error while storing SCM signed certificate.", e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.error("Error while storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    } catch (IOException | CertificateException  e) {
      LOG.error("Error while storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }

  }

  /**
   * This function creates/initializes a certificate server as needed.
   * This function is idempotent, so calling this again and again after the
   * server is initialized is not a problem.
   *
   * @param clusterID - Cluster ID
   * @param scmID     - SCM ID
   */
  public static CertificateServer initializeRootCertificateServer(
      String clusterID, String scmID, SCMCertStore scmCertStore)
      throws IOException {
    String subject = "scm-rootca@" + InetAddress.getLocalHost().getHostName();

    return new DefaultCAServer(subject, clusterID, scmID, scmCertStore,
        new DefaultCAProfile(),
        Paths.get(SCM_CA_CERT_STORAGE_DIR, SCM_CA_PATH).toString());
  }

  private static PKCS10CertificationRequest generateCSR(
      CertificateClient client, SCMStorageConfig scmStorageConfig,
      OzoneConfiguration config, InetSocketAddress scmAddress,
      String fetchedSCMId) throws IOException {
    CertificateSignRequest.Builder builder = client.getCSRBuilder();
    KeyPair keyPair = new KeyPair(client.getPublicKey(),
        client.getPrivateKey());

    // Get host name.
    String hostname = scmAddress.getAddress().getHostName();

    String subject = "scm@"+ hostname;

    builder.setKey(keyPair)
        .setConfiguration(config)
        .setScmID(fetchedSCMId)
        .setClusterID(scmStorageConfig.getClusterID())
        .setSubject(subject);


    LOG.info("Creating csr for SCM->hostName:{},scmId:{},clusterId:{}," +
            "subject:{}", hostname, fetchedSCMId,
        scmStorageConfig.getClusterID(), subject);

    return builder.build();
  }
}
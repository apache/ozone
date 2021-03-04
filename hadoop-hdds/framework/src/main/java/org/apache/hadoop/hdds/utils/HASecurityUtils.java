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
package org.apache.hadoop.hdds.utils;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyPair;

import static org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest.getEncodedString;

public final class HASecurityUtils {

  private HASecurityUtils() {
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(HASecurityUtils.class);

  /**
   * Initialize Security which generates public, private key pair and get SCM
   * signed certificate and persist to local disk.
   * @param clusterId
   * @param fetchedScmId
   * @param conf
   * @param scmAddress
   * @throws IOException
   */
  public static void initializeSecurity(String clusterId,
      String fetchedScmId, OzoneConfiguration conf,
      InetSocketAddress scmAddress)
      throws IOException {
    LOG.info("Initializing secure StorageContainerManager.");

    CertificateClient certClient =
        new SCMCertificateClient(new SecurityConfig(conf));
    CertificateClient.InitResponse response = certClient.init();
    LOG.info("Init response: {}", response);
    switch (response) {
    case SUCCESS:
      LOG.info("Initialization successful.");
      break;
    case GETCERT:
      getSCMSignedCert(certClient, conf, fetchedScmId, clusterId, scmAddress);
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
  private static void getSCMSignedCert(CertificateClient client,
      OzoneConfiguration config, String fetchedSCMId, String clusterId,
      InetSocketAddress scmAddress) throws IOException {
    CertificateSignRequest.Builder builder = client.getCSRBuilder();
    KeyPair keyPair = new KeyPair(client.getPublicKey(),
        client.getPrivateKey());

    // Get host name.
    String hostname = scmAddress.getAddress().getHostName();
    String ip = scmAddress.getAddress().getHostAddress();

    String subject;
    if (builder.hasDnsName()) {
      subject = UserGroupInformation.getCurrentUser().getShortUserName()
          + "@" + hostname;
    } else {
      // With only IP in alt.name, certificate validation would fail if subject
      // isn't a hostname either, so omit username.
      subject = hostname;
    }

    builder.setKey(keyPair)
        .setConfiguration(config)
        .setScmID(fetchedSCMId)
        .setClusterID(clusterId)
        .setSubject(subject);


    LOG.info("Creating csr for SCM->hostName:{},ip:{},scmId:{},clusterId:{}," +
            "subject:{}", hostname, ip, fetchedSCMId, clusterId, subject);

    HddsProtos.ScmNodeDetailsProto scmNodeDetailsProto =
        HddsProtos.ScmNodeDetailsProto.newBuilder()
            .setClusterId(clusterId)
            .setHostName(scmAddress.getHostName())
            .setScmNodeId(fetchedSCMId).build();

    PKCS10CertificationRequest csr = builder.build();

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
      } else {
        throw new RuntimeException("Unable to retrieve SCM certificate chain");
      }
    } catch (IOException e) {
      LOG.error("Error while storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }

  }
}

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

package org.apache.hadoop.ozone.security;

import java.io.IOException;
import java.security.KeyPair;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.client.DefaultCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Certificate client for OzoneManager.
 */
public class OMCertificateClient extends DefaultCertificateClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMCertificateClient.class);

  public static final String COMPONENT_NAME = "om";
  private String serviceId;
  private String scmID;
  private final String clusterID;
  private final HddsProtos.OzoneManagerDetailsProto omInfo;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMCertificateClient(
      SecurityConfig secConfig,
      SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient,
      OMStorage omStorage,
      HddsProtos.OzoneManagerDetailsProto omInfo,
      String serviceId,
      String scmID,
      Consumer<String> saveCertIdCallback,
      Runnable shutdownCallback
  ) {
    super(secConfig, scmSecurityClient, LOG, omStorage.getOmCertSerialId(),
        COMPONENT_NAME, HddsUtils.threadNamePrefix(omStorage.getOmNodeId()),
        saveCertIdCallback, shutdownCallback);
    this.serviceId = serviceId;
    this.scmID = scmID;
    this.clusterID = omStorage.getClusterID();
    this.omInfo = omInfo;
  }

  /**
   * Returns a CSR builder that can be used to create a Certificate sigining
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  @Override
  public CertificateSignRequest.Builder configureCSRBuilder()
      throws SCMSecurityException {
    CertificateSignRequest.Builder builder = super.configureCSRBuilder();

    String hostname = omInfo.getHostName();
    String subject;
    if (builder.hasDnsName()) {
      try {
        subject = UserGroupInformation.getCurrentUser().getShortUserName()
            + "@" + hostname;
      } catch (IOException e) {
        throw new CertificateException("Failed to getCurrentUser", e);
      }
    } else {
      // With only IP in alt.name, certificate validation would fail if subject
      // isn't a hostname either, so omit username.
      subject = hostname;
    }

    builder.setCA(false)
        .setKey(new KeyPair(getPublicKey(), getPrivateKey()))
        .setConfiguration(getSecurityConfig())
        .setScmID(scmID)
        .setClusterID(clusterID)
        .setSubject(subject);

    if (!StringUtils.isEmpty(serviceId)) {
      builder.addServiceName(serviceId);
    }

    LOG.info("Creating csr for OM->dns:{},ip:{},scmId:{},clusterId:{}," +
            "subject:{}", hostname, omInfo.getIpAddress(), scmID, clusterID,
        subject);
    return builder;
  }

  @Override
  protected SCMGetCertResponseProto sign(CertificateSignRequest request) throws IOException {
    return getScmSecureClient().getOMCertChain(omInfo, request.toEncodedFormat());
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }
}

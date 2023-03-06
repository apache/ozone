/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.security;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.client.CommonCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ha.OMHANodeDetails;
import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.function.Consumer;

import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest.getEncodedString;

/**
 * Certificate client for OzoneManager.
 */
public class OMCertificateClient extends CommonCertificateClient {

  public static final Logger LOG =
      LoggerFactory.getLogger(OMCertificateClient.class);

  public static final String COMPONENT_NAME = "om";
  private String scmID;
  private final String clusterID;
  private final HddsProtos.OzoneManagerDetailsProto omInfo;

  @SuppressWarnings("parameternumber")
  public OMCertificateClient(SecurityConfig secConfig, String scmId,
      String clusterId, HddsProtos.OzoneManagerDetailsProto omDetails,
      String certSerialId, String localCrlId,
      Consumer<String> persistCertIdCallback, Runnable shutdownCallback) {
    super(secConfig, LOG, certSerialId, COMPONENT_NAME, persistCertIdCallback,
        shutdownCallback);
    this.setLocalCrlId(localCrlId != null ?
        Long.parseLong(localCrlId) : 0);
    this.scmID = scmId;
    this.clusterID = clusterId;
    this.omInfo = omDetails;
  }

  public OMCertificateClient(SecurityConfig secConfig,
      OMStorage omStorage, String scmID, Consumer<String> saveCertIdCallback,
      Runnable shutdownCallback) {
    this(secConfig, scmID, omStorage.getClusterID(),
        OzoneManager.getOmDetailsProto(
            (OzoneConfiguration) secConfig.getConfiguration(),
            omStorage.getOmId()),
        omStorage.getOmCertSerialId(), null,
        saveCertIdCallback, shutdownCallback);
  }

  public OMCertificateClient(SecurityConfig secConfig, OMStorage omStorage,
      String scmID) {
    this(secConfig, scmID, omStorage.getClusterID(),
        OzoneManager.getOmDetailsProto(
            (OzoneConfiguration) secConfig.getConfiguration(),
            omStorage.getOmId()),
        omStorage.getOmCertSerialId(), null, null, null);
  }

  public OMCertificateClient(SecurityConfig secConfig) {
    this(secConfig, null, null, null, null, null, null, null);
  }

  public OMCertificateClient(SecurityConfig secConfig, String certSerialId) {
    this(secConfig, null, null, null, certSerialId, null, null, null);
  }

  /**
   * Returns a CSR builder that can be used to create a Certificate signing
   * request.
   * The default flag is added to allow basic SSL handshake.
   *
   * @return CertificateSignRequest.Builder
   */
  @Override
  public CertificateSignRequest.Builder getCSRBuilder()
      throws CertificateException {
    return getCSRBuilder(new KeyPair(getPublicKey(), getPrivateKey()));
  }

  /**
   * Returns a CSR builder that can be used to create a Certificate sigining
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  @Override
  public CertificateSignRequest.Builder getCSRBuilder(KeyPair keyPair)
      throws CertificateException {
    CertificateSignRequest.Builder builder = super.getCSRBuilder()
        .setDigitalEncryption(true)
        .setDigitalSignature(true);

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
        .setKey(keyPair)
        .setConfiguration(getConfig())
        .setScmID(scmID)
        .setClusterID(clusterID)
        .setSubject(subject);

    OMHANodeDetails haOMHANodeDetails =
        OMHANodeDetails.loadOMHAConfig(getConfig());
    String serviceName =
        haOMHANodeDetails.getLocalNodeDetails().getServiceId();
    if (!StringUtils.isEmpty(serviceName)) {
      builder.addServiceName(serviceName);
    }

    LOG.info("Creating csr for OM->dns:{},ip:{},scmId:{},clusterId:{}," +
            "subject:{}", hostname, omInfo.getIpAddress(), scmID, clusterID,
        subject);
    return builder;
  }

  @Override
  public String signAndStoreCertificate(PKCS10CertificationRequest request,
      Path certificatePath) throws CertificateException {
    try {
      SCMGetCertResponseProto response = getScmSecureClient()
          .getOMCertChain(omInfo, getEncodedString(request));

      String pemEncodedCert = response.getX509Certificate();
      CertificateCodec certCodec = new CertificateCodec(
          getSecurityConfig(), certificatePath);

      // Store SCM CA certificate.
      if (response.hasX509CACertificate()) {
        String pemEncodedRootCert = response.getX509CACertificate();
        storeCertificate(pemEncodedRootCert,
            CAType.SUBORDINATE, certCodec, false);
        storeCertificate(pemEncodedCert, CAType.NONE, certCodec,
            false);

        // Store Root CA certificate if available.
        if (response.hasX509RootCACertificate()) {
          storeCertificate(response.getX509RootCACertificate(),
              CAType.ROOT, certCodec, false);
        }
        return CertificateCodec.getX509Certificate(pemEncodedCert)
            .getSerialNumber().toString();
      } else {
        throw new CertificateException("Unable to retrieve OM certificate " +
            "chain.");
      }
    } catch (IOException | java.security.cert.CertificateException e) {
      LOG.error("Error while signing and storing SCM signed certificate.", e);
      throw new CertificateException(
          "Error while signing and storing SCM signed certificate.", e);
    }
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }
}

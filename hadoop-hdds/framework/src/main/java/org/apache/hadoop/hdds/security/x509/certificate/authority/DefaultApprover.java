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

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest.getDistinguishedNameWithSN;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest.getPkcs9Extensions;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.math.BigInteger;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.PKIProfile;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.pkcs.Attribute;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.params.RSAKeyParameters;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.crypto.util.PublicKeyFactory;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.ContentVerifierProvider;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.bouncycastle.operator.jcajce.JcaContentVerifierProviderBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Approver used the by the DefaultCA.
 */
public class DefaultApprover implements CertificateApprover {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultApprover.class);

  private final PKIProfile profile;
  private final SecurityConfig securityConfig;

  /**
   * Constructs the Default Approver.
   *
   * @param pkiProfile - PKI Profile to use.
   * @param config - Security Config
   */
  public DefaultApprover(PKIProfile pkiProfile, SecurityConfig config) {
    this.profile = Objects.requireNonNull(pkiProfile);
    this.securityConfig = Objects.requireNonNull(config);
  }

  /**
   * Sign function signs a Certificate.
   *
   * @param config - Security Config.
   * @param caPrivate - CAs private Key.
   * @param caCertificate - CA Certificate.
   * @param validFrom - Begin Da te
   * @param validTill - End Date
   * @param certificationRequest - Certification Request.
   * @param scmId - SCM id.
   * @param clusterId - Cluster id.
   * @param certSerialId - the new certificate id.
   * @return Signed Certificate.
   * @throws IOException - On Error
   * @throws CertificateException - on Error.
   */
  @SuppressWarnings("ParameterNumber")
  @Override
  public X509Certificate sign(
      SecurityConfig config,
      PrivateKey caPrivate,
      X509Certificate caCertificate,
      Date validFrom,
      Date validTill,
      PKCS10CertificationRequest certificationRequest,
      String scmId,
      String clusterId,
      String certSerialId) throws IOException, CertificateException {

    AlgorithmIdentifier sigAlgId = new
        DefaultSignatureAlgorithmIdentifierFinder().find(
        config.getSignatureAlgo());
    AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder()
        .find(sigAlgId);

    AsymmetricKeyParameter asymmetricKP = PrivateKeyFactory.createKey(caPrivate
        .getEncoded());
    SubjectPublicKeyInfo keyInfo =
        certificationRequest.getSubjectPublicKeyInfo();

    // Get scmId and cluster Id from subject name.
    X500Name x500Name = certificationRequest.getSubject();
    String csrScmId = x500Name.getRDNs(BCStyle.OU)[0].getFirst().getValue().
        toASN1Primitive().toString();
    String csrClusterId = x500Name.getRDNs(BCStyle.O)[0].getFirst().getValue().
        toASN1Primitive().toString();
    String cn = x500Name.getRDNs(BCStyle.CN)[0].getFirst().getValue()
        .toASN1Primitive().toString();

    if (!clusterId.equals(csrClusterId)) {
      if (csrScmId.equalsIgnoreCase("null") &&
          csrClusterId.equalsIgnoreCase("null")) {
        // Special case to handle DN certificate generation as DN might not know
        // scmId and clusterId before registration. In secure mode registration
        // will succeed only after datanode has a valid certificate.
        csrClusterId = clusterId;
        csrScmId = scmId;
      } else {
        // Throw exception if scmId and clusterId doesn't match.
        throw new SCMSecurityException("ScmId and ClusterId in CSR subject" +
            " are incorrect.");
      }
    }
    x500Name = getDistinguishedNameWithSN(cn, csrScmId, csrClusterId,
        certSerialId);

    RSAKeyParameters rsa =
        (RSAKeyParameters) PublicKeyFactory.createKey(keyInfo);
    if (rsa.getModulus().bitLength() < config.getSize()) {
      throw new SCMSecurityException("Key size is too small in certificate " +
          "signing request");
    }
    X509v3CertificateBuilder certificateGenerator =
        new X509v3CertificateBuilder(
            new X509CertificateHolder(caCertificate.getEncoded()).getSubject(),
            new BigInteger(certSerialId),
            validFrom,
            validTill,
            x500Name, keyInfo);

    Extensions exts = getPkcs9Extensions(certificationRequest);
    LOG.info("Extensions in CSR: {}",
        Arrays.stream(exts.getExtensionOIDs())
            .map(ASN1ObjectIdentifier::getId)
            .collect(Collectors.joining(", ")));
    LOG.info("Extensions to add to the certificate if they present in CSR: {}",
        Arrays.stream(profile.getSupportedExtensions())
            .map(oid -> oid == null ? "null" : oid.getId())
            .collect(Collectors.joining(", ")));
    for (ASN1ObjectIdentifier extId : profile.getSupportedExtensions()) {
      Extension ext = exts.getExtension(extId);
      if (ext != null) {
        certificateGenerator.addExtension(ext);
      }
    }

    try {
      ContentSigner sigGen = new BcRSAContentSignerBuilder(sigAlgId, digAlgId)
          .build(asymmetricKP);

      //TODO: as part of HDDS-10743 ensure that converter is instantiated only once
      return new JcaX509CertificateConverter().getCertificate(certificateGenerator.build(sigGen));
    } catch (OperatorCreationException oce) {
      throw new CertificateException(oce);
    }
  }

  /**
   * Returns the Attribute array that encodes extensions.
   *
   * @param request - Certificate Request
   * @return - An Array of Attributes that encode various extensions requested
   * in this certificate.
   */
  private Attribute[] getAttributes(PKCS10CertificationRequest request) {
    Objects.requireNonNull(request);

    return request.getAttributes(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest);
  }

  /**
   * Returns a list of Extensions encoded in a given attribute.
   *
   * @param attribute - Attribute to decode.
   * @return - List of Extensions.
   */
  private List<Extensions> getExtensionsList(Attribute attribute) {
    Objects.requireNonNull(attribute);
    List<Extensions> extensionsList = new ArrayList<>();
    for (ASN1Encodable value : attribute.getAttributeValues()) {
      if (value != null) {
        Extensions extensions = Extensions.getInstance(value);
        extensionsList.add(extensions);
      }
    }
    return extensionsList;
  }

  /**
   * Returns the Extension decoded into a Java Collection.
   * @param extensions - A set of Extensions in ASN.1.
   * @return List of Decoded Extensions.
   */
  private List<Extension> getIndividualExtension(Extensions extensions) {
    Objects.requireNonNull(extensions);
    List<Extension> extenList = new ArrayList<>();
    for (ASN1ObjectIdentifier id : extensions.getExtensionOIDs()) {
      if (id != null) {
        Extension ext = extensions.getExtension(id);
        if (ext != null) {
          extenList.add(ext);
        }
      }
    }
    return extenList;
  }

  /**
   * This function verifies all extensions in the certificate.
   *
   * @param request - CSR
   * @return - true if the extensions are acceptable by the profile, false
   * otherwise.
   */
  @VisibleForTesting
  boolean verfiyExtensions(PKCS10CertificationRequest request) {
    Objects.requireNonNull(request);
    /*
     * Inside a CSR we have
     *  1. A list of Attributes
     *    2. Inside each attribute a list of extensions.
     *      3. We need to walk thru the each extension and verify they
     *      are expected and we can put that into a certificate.
     */

    for (Attribute attr : getAttributes(request)) {
      for (Extensions extensionsList : getExtensionsList(attr)) {
        for (Extension extension : getIndividualExtension(extensionsList)) {
          if (!profile.validateExtension(extension)) {
            LOG.error("Failed to verify extension. {}",
                extension.getExtnId().getId());
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Verifies the Signature on the CSR is valid.
   *
   * @param pkcs10Request - PCKS10 Request.
   * @return True if it is valid, false otherwise.
   * @throws OperatorCreationException - On Error.
   * @throws PKCSException             - on Error.
   */
  @VisibleForTesting
  boolean verifyPkcs10Request(PKCS10CertificationRequest pkcs10Request)
      throws OperatorCreationException, PKCSException {
    ContentVerifierProvider verifierProvider = new
        JcaContentVerifierProviderBuilder()
        .setProvider(this.securityConfig.getProvider())
        .build(pkcs10Request.getSubjectPublicKeyInfo());
    return
        pkcs10Request.isSignatureValid(verifierProvider);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> inspectCSR(PKCS10CertificationRequest csr) {
    /**
     * The base approver executes the following algorithm to verify that a
     * CSR meets the PKI Profile criteria.
     *
     * 0. For time being (Until we have SCM HA) we will deny all request to
     * become an intermediary CA. So we will not need to verify using CA
     * profile, right now.
     *
     * 1. We verify the proof of possession. That is we verify the entity
     * that sends us the CSR indeed has the private key for the said public key.
     *
     * 2. Then we will verify the RDNs meet the format and the Syntax that
     * PKI profile dictates.
     *
     * 3. Then we decode each and every extension and  ask if the PKI profile
     * approves of these extension requests.
     *
     * 4. If all of these pass, We will return a Future which will point to
     * the Certificate when finished.
     */

    CompletableFuture<Void> response = new CompletableFuture<>();
    try {
      // Step 0: Verify this is not a CA Certificate.
      // Will be done by the Ozone PKI profile for time being.
      // If there are any basicConstraints, they will flagged as not
      // supported for time being.

      // Step 1: Let us verify that Certificate is indeed signed by someone
      // who has access to the private key.
      if (!verifyPkcs10Request(csr)) {
        LOG.error("Failed to verify the signature in CSR.");
        response.completeExceptionally(new SCMSecurityException("Failed to " +
            "verify the CSR."));
      }

      // Step 2: Verify the RDNs are in the correct format.
      // TODO: Ozone Profile does not verify RDN now, so this call will pass.
      for (RDN rdn : csr.getSubject().getRDNs()) {
        if (!profile.validateRDN(rdn)) {
          LOG.error("Failed in verifying RDNs");
          response.completeExceptionally(new SCMSecurityException("Failed to " +
              "verify the RDNs. Please check the subject name."));
        }
      }

      // Step 3: Verify the Extensions.
      if (!verfiyExtensions(csr)) {
        LOG.error("failed in verification of extensions.");
        response.completeExceptionally(new SCMSecurityException("Failed to " +
            "verify extensions."));
      }

    } catch (OperatorCreationException | PKCSException e) {
      LOG.error("Approval Failure.", e);
      response.completeExceptionally(new SCMSecurityException(e));
    }
    return response;
  }
}

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

package org.apache.hadoop.hdds.security.x509.certificate.utils;

import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.INVALID_CSR;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.CSR_ERROR;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.InetAddress;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.bouncycastle.asn1.ASN1EncodableVector;
import org.bouncycastle.asn1.ASN1Object;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.ASN1Set;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.DERTaggedObject;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.pkcs.Attribute;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A certificate sign request object that wraps operations to build a
 * PKCS10CertificationRequest to CertificateServer.
 */
public final class CertificateSignRequest {
  // Ozone final certificate distinguished format:
  // (CN=Subject,OU=ScmID,O=ClusterID,SERIALNUMBER=SerialID).
  private static final String DISTINGUISHED_NAME_FORMAT = "CN=%s,OU=%s,O=%s";
  private static final String DISTINGUISHED_NAME_WITH_SN_FORMAT =
      "CN=%s,OU=%s,O=%s,SERIALNUMBER=%s";
  private static final Logger LOG =
      LoggerFactory.getLogger(CertificateSignRequest.class);
  private final KeyPair keyPair;
  private final SecurityConfig config;
  private final Extensions extensions;
  private String subject;
  private String clusterID;
  private String scmID;

  /**
   * Private Ctor for CSR.
   *
   * @param subject - Subject
   * @param scmID - SCM ID
   * @param clusterID - Cluster ID
   * @param keyPair - KeyPair
   * @param config - SCM Config
   * @param extensions - CSR extensions
   */
  private CertificateSignRequest(String subject, String scmID, String clusterID,
                                 KeyPair keyPair, SecurityConfig config,
                                 Extensions extensions) {
    this.subject = subject;
    this.clusterID = clusterID;
    this.scmID = scmID;
    this.keyPair = keyPair;
    this.config = config;
    this.extensions = extensions;
  }

  public static String getDistinguishedNameFormat() {
    return DISTINGUISHED_NAME_FORMAT;
  }

  public static String getDistinguishedNameFormatWithSN() {
    return DISTINGUISHED_NAME_WITH_SN_FORMAT;
  }

  // used by server side DN regeneration
  public static X500Name getDistinguishedNameWithSN(String subject,
      String scmID, String clusterID, String serialID) {
    return new X500Name(String.format(DISTINGUISHED_NAME_WITH_SN_FORMAT,
        subject, scmID, clusterID, serialID));
  }

  // used by client side DN generation
  public static X500Name getDistinguishedName(String subject, String scmID,
      String clusterID) {
    return new X500Name(String.format(getDistinguishedNameFormat(), subject,
        scmID, clusterID));
  }

  public static Extensions getPkcs9Extensions(PKCS10CertificationRequest csr)
      throws CertificateException {
    ASN1Set pkcs9ExtReq = getPkcs9ExtRequest(csr);
    Object extReqElement = pkcs9ExtReq.getObjects().nextElement();
    if (extReqElement instanceof Extensions) {
      return (Extensions) extReqElement;
    } else {
      if (extReqElement instanceof ASN1Sequence) {
        return Extensions.getInstance((ASN1Sequence) extReqElement);
      } else {
        throw new CertificateException("Unknown element type :" + extReqElement
            .getClass().getSimpleName());
      }
    }
  }

  public static ASN1Set getPkcs9ExtRequest(PKCS10CertificationRequest csr)
      throws CertificateException {
    for (Attribute attr : csr.getAttributes()) {
      ASN1ObjectIdentifier oid = attr.getAttrType();
      if (oid.equals(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest)) {
        return attr.getAttrValues();
      }
    }
    throw new CertificateException("No PKCS#9 extension found in CSR");
  }

  /**
   * Encodes this CertificateSignRequest to a String representation, that can be transferred over the wire to
   * the CA server for signing.
   *
   * @return the Certificate Sign Request encoded to a String
   * @throws IOException if an error occurs during encoding.
   */
  public String toEncodedFormat() throws IOException {
    StringWriter str = new StringWriter();
    try (JcaPEMWriter pemWriter = new JcaPEMWriter(str)) {
      PemObject pemObject = new PemObject("CERTIFICATE REQUEST", generateCSR().getEncoded());
      pemWriter.writeObject(pemObject);
    }
    return str.toString();
  }

  //TODO: this should be private once the server side of removing PKCS10CertReq class is done.
  public PKCS10CertificationRequest generateCSR() throws IOException {
    X500Name dnName = getDistinguishedName(subject, scmID, clusterID);
    PKCS10CertificationRequestBuilder p10Builder =
        new JcaPKCS10CertificationRequestBuilder(dnName, keyPair.getPublic());

    try {
      ContentSigner contentSigner =
          new JcaContentSignerBuilder(config.getSignatureAlgo())
              .setProvider(config.getProvider())
              .build(keyPair.getPrivate());

      if (extensions != null) {
        p10Builder.addAttribute(
            PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, extensions);
      }
      return p10Builder.build(contentSigner);
    } catch (OperatorCreationException e) {
      throw new IOException(e);
    }
  }

  /**
   * Gets a CertificateRequest Object from PEM encoded CSR.
   *
   * @param csr - PEM Encoded Certificate Request String.
   * @return PKCS10CertificationRequest
   * @throws IOException - On Error.
   */
  public static PKCS10CertificationRequest getCertificationRequest(String csr)
      throws IOException {
    try (PemReader reader = new PemReader(new StringReader(csr))) {
      PemObject pemObject = reader.readPemObject();
      if (pemObject.getContent() == null) {
        throw new SCMSecurityException("Invalid Certificate signing request",
            INVALID_CSR);
      }
      return new PKCS10CertificationRequest(pemObject.getContent());
    }
  }

  /**
   * Builder class for Certificate Sign Request.
   */
  public static class Builder {
    private String subject;
    private String clusterID;
    private String scmID;
    private KeyPair key;
    private SecurityConfig config;
    private List<GeneralName> altNames;
    private Boolean ca = false;
    private boolean digitalSignature;
    private boolean digitalEncryption;

    public CertificateSignRequest.Builder setConfiguration(
        SecurityConfig configuration) {
      this.config = configuration;
      return this;
    }

    public CertificateSignRequest.Builder setKey(KeyPair keyPair) {
      this.key = keyPair;
      return this;
    }

    public CertificateSignRequest.Builder setSubject(String subjectString) {
      this.subject = subjectString;
      return this;
    }

    public CertificateSignRequest.Builder setClusterID(String s) {
      this.clusterID = s;
      return this;
    }

    public CertificateSignRequest.Builder setScmID(String s) {
      this.scmID = s;
      return this;
    }

    public Builder setDigitalSignature(boolean dSign) {
      this.digitalSignature = dSign;
      return this;
    }

    public Builder setDigitalEncryption(boolean dEncryption) {
      this.digitalEncryption = dEncryption;
      return this;
    }

    // Support SAN extension with DNS and RFC822 Name
    // other name type will be added as needed.
    public CertificateSignRequest.Builder addDnsName(String dnsName) {
      Objects.requireNonNull(dnsName, "dnsName cannot be null");
      this.addAltName(GeneralName.dNSName, dnsName);
      return this;
    }

    public boolean hasDnsName() {
      if (altNames == null || altNames.isEmpty()) {
        return false;
      }
      for (GeneralName name : altNames) {
        if (name.getTagNo() == GeneralName.dNSName) {
          return true;
        }
      }
      return false;
    }

    // IP address is subject to change which is optional for now.
    public CertificateSignRequest.Builder addIpAddress(String ip) {
      Objects.requireNonNull(ip, "Ip address cannot be null");
      this.addAltName(GeneralName.iPAddress, ip);
      return this;
    }

    public CertificateSignRequest.Builder addInetAddresses()
        throws CertificateException {
      try {
        DomainValidator validator = DomainValidator.getInstance();
        // Add all valid ips.
        List<InetAddress> inetAddresses =
            OzoneSecurityUtil.getValidInetsForCurrentHost();
        this.addInetAddresses(inetAddresses, validator);
      } catch (IOException e) {
        throw new CertificateException("Error while getting Inet addresses " +
            "for the CSR builder", e, CSR_ERROR);
      }
      return this;
    }

    public CertificateSignRequest.Builder addInetAddresses(
        List<InetAddress> addresses,
        DomainValidator validator) {
      // Add all valid ips.
      addresses.forEach(
          ip -> {
            this.addIpAddress(ip.getHostAddress());
            if (validator.isValid(ip.getCanonicalHostName())) {
              this.addDnsName(ip.getCanonicalHostName());
            } else {
              LOG.error("Invalid domain {}", ip.getCanonicalHostName());
            }
          });
      return this;
    }

    public CertificateSignRequest.Builder addServiceName(
        String serviceName) {
      Objects.requireNonNull(serviceName, "Service Name cannot be null");

      this.addAltName(GeneralName.otherName, serviceName);
      return this;
    }

    private CertificateSignRequest.Builder addAltName(int tag, String name) {
      if (altNames == null) {
        altNames = new ArrayList<>();
      }
      if (tag == GeneralName.otherName) {
        ASN1Object ono = addOtherNameAsn1Object(name);

        altNames.add(new GeneralName(tag, ono));
      } else {
        altNames.add(new GeneralName(tag, name));
      }
      return this;
    }

    /**
     * addOtherNameAsn1Object requires special handling since
     * Bouncy Castle does not support othername as string.
     * @param name
     * @return
     */
    private ASN1Object addOtherNameAsn1Object(String name) {
      // Below oid is copied from this URL:
      // https://docs.microsoft.com/en-us/windows/win32/adschema/a-middlename
      final String otherNameOID = "2.16.840.1.113730.3.1.34";
      ASN1EncodableVector otherName = new ASN1EncodableVector();
      otherName.add(new ASN1ObjectIdentifier(otherNameOID));
      otherName.add(new DERTaggedObject(
          true, GeneralName.otherName, new DERUTF8String(name)));
      return new DERTaggedObject(
          false, 0, new DERSequence(otherName));
    }

    public CertificateSignRequest.Builder setCA(Boolean isCA) {
      this.ca = isCA;
      return this;
    }

    private Extension getKeyUsageExtension() throws IOException {
      int keyUsageFlag = KeyUsage.keyAgreement;
      if (digitalEncryption) {
        keyUsageFlag |= KeyUsage.keyEncipherment | KeyUsage.dataEncipherment;
      }
      if (digitalSignature) {
        keyUsageFlag |= KeyUsage.digitalSignature;
      }

      if (ca) {
        keyUsageFlag |= KeyUsage.keyCertSign | KeyUsage.cRLSign;
      }
      KeyUsage keyUsage = new KeyUsage(keyUsageFlag);
      return new Extension(Extension.keyUsage, true,
          keyUsage.getEncoded());
    }

    private Optional<Extension> getSubjectAltNameExtension() throws
        IOException {
      if (altNames != null) {
        return Optional.of(new Extension(Extension.subjectAlternativeName,
            false, new DEROctetString(new GeneralNames(
            altNames.toArray(new GeneralName[0])))));
      }
      return Optional.empty();
    }

    private Extension getBasicExtension() throws IOException {
      // We don't set pathLenConstraint means no limit is imposed.
      return new Extension(Extension.basicConstraints,
          true, new DEROctetString(new BasicConstraints(ca)));
    }

    private Extensions createExtensions() throws IOException {
      List<Extension> extensions = new ArrayList<>();

      // Add basic extension
      if (ca) {
        extensions.add(getBasicExtension());
      }

      // Add key usage extension
      extensions.add(getKeyUsageExtension());

      // Add subject alternate name extension
      Optional<Extension> san = getSubjectAltNameExtension();
      san.ifPresent(extensions::add);

      return new Extensions(
          extensions.toArray(new Extension[0]));
    }

    public CertificateSignRequest build() throws SCMSecurityException {
      Objects.requireNonNull(key, "KeyPair cannot be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(subject), "Subject " +
          "cannot be blank");

      try {
        CertificateSignRequest csr = new CertificateSignRequest(subject, scmID,
            clusterID, key, config, createExtensions());
        return csr;
      } catch (IOException ioe) {
        throw new CertificateException(String.format("Unable to create " +
            "extension for certificate sign request for %s.",
            getDistinguishedName(subject, scmID, clusterID)), ioe.getCause());
      }
    }
  }
}

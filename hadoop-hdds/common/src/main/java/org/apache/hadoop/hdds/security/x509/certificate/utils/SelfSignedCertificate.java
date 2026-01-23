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

import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.CERTIFICATE_ERROR;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.CSR_ERROR;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.util.Time;
import org.bouncycastle.asn1.ASN1EncodableVector;
import org.bouncycastle.asn1.ASN1Object;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.DERTaggedObject;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Self Signed Certificate with CertificateServer basic constraint can be used
 * to bootstrap a certificate infrastructure, if no external certificate is
 * provided.
 */
public final class SelfSignedCertificate {
  private String subject;
  private String clusterID;
  private String scmID;
  private ZonedDateTime beginDate;
  private ZonedDateTime endDate;
  private KeyPair key;
  private SecurityConfig config;
  private List<GeneralName> altNames;
  private static final Logger LOG =
      LoggerFactory.getLogger(SelfSignedCertificate.class);

  /**
   * Private Ctor invoked only via Builder Interface.
   *
   * @param builder - builder
   */

  private SelfSignedCertificate(Builder builder) {
    this.subject = builder.subject;
    this.clusterID = builder.clusterID;
    this.scmID = builder.scmID;
    this.beginDate = builder.beginDate;
    this.endDate = builder.endDate;
    this.config = builder.config;
    this.key = builder.key;
    this.altNames = builder.altNames;
  }

  @VisibleForTesting
  public static String getNameFormat() {
    return CertificateSignRequest.getDistinguishedNameFormatWithSN();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private X509Certificate generateCertificate(BigInteger caCertSerialId) throws OperatorCreationException, IOException {
    byte[] encoded = key.getPublic().getEncoded();
    SubjectPublicKeyInfo publicKeyInfo =
        SubjectPublicKeyInfo.getInstance(encoded);


    ContentSigner contentSigner =
        new JcaContentSignerBuilder(config.getSignatureAlgo())
            .setProvider(config.getProvider()).build(key.getPrivate());

    BigInteger serial;
    if (caCertSerialId == null) {
      serial = new BigInteger(Long.toString(Time.monotonicNow()));
    } else {
      serial = caCertSerialId;
    }
    // For the Root Certificate we form the name from Subject, SCM ID and
    // Cluster ID.
    String dnName = String.format(getNameFormat(),
        subject, scmID, clusterID, serial);
    X500Name name = new X500Name(dnName);

    // Valid from the Start of the day when we generate this Certificate.
    Date validFrom = Date.from(beginDate.toInstant());

    // Valid till end day finishes.
    Date validTill = Date.from(endDate.toInstant());

    X509v3CertificateBuilder builder = new X509v3CertificateBuilder(name,
        serial, validFrom, validTill, name, publicKeyInfo);

    if (caCertSerialId != null) {
      builder.addExtension(Extension.basicConstraints, true,
          new BasicConstraints(true));
      int keyUsageFlag = KeyUsage.keyCertSign | KeyUsage.cRLSign;
      KeyUsage keyUsage = new KeyUsage(keyUsageFlag);
      builder.addExtension(Extension.keyUsage, true, keyUsage);
      if (altNames != null && !altNames.isEmpty()) {
        builder.addExtension(new Extension(Extension.subjectAlternativeName,
            false, new GeneralNames(altNames.toArray(
                new GeneralName[altNames.size()])).getEncoded()));
      }
    }
    try {
      //TODO: as part of HDDS-10743 ensure that converter is instantiated only once
      X509Certificate cert = new JcaX509CertificateConverter().getCertificate(builder.build(contentSigner));
      LOG.info("Certificate {} is issued by {} to {}, valid from {} to {}",
          cert.getSerialNumber(), cert.getIssuerDN(), cert.getSubjectDN(), cert.getNotBefore(), cert.getNotAfter());
      return cert;
    } catch (java.security.cert.CertificateException e) {
      throw new CertificateException("Could not create self-signed X509Certificate.", e, CERTIFICATE_ERROR);
    }
  }

  /**
   * Builder class for Root Certificates.
   */
  public static class Builder {
    private String subject;
    private String clusterID;
    private String scmID;
    private ZonedDateTime beginDate;
    private ZonedDateTime endDate;
    private KeyPair key;
    private SecurityConfig config;
    private BigInteger caCertSerialId;
    private List<GeneralName> altNames;

    public Builder setConfiguration(SecurityConfig configuration) {
      this.config = configuration;
      return this;
    }

    public Builder setKey(KeyPair keyPair) {
      this.key = keyPair;
      return this;
    }

    public Builder setSubject(String subjectString) {
      this.subject = subjectString;
      return this;
    }

    public Builder setClusterID(String s) {
      this.clusterID = s;
      return this;
    }

    public Builder setScmID(String s) {
      this.scmID = s;
      return this;
    }

    public Builder setBeginDate(ZonedDateTime date) {
      this.beginDate = date;
      return this;
    }

    public Builder setEndDate(ZonedDateTime date) {
      this.endDate = date;
      return this;
    }

    public Builder makeCA() {
      return makeCA(BigInteger.ONE);
    }

    public Builder makeCA(BigInteger serialId) {
      this.caCertSerialId = serialId;
      return this;
    }

    public Builder addInetAddresses() throws CertificateException {
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

    public Builder addInetAddresses(List<InetAddress> addresses,
        DomainValidator validator) {
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

    // Support SAN extension with DNS and RFC822 Name
    // other name type will be added as needed.
    public Builder addDnsName(String dnsName) {
      Objects.requireNonNull(dnsName, "dnsName cannot be null");
      this.addAltName(GeneralName.dNSName, dnsName);
      return this;
    }

    // IP address is subject to change which is optional for now.
    public Builder addIpAddress(String ip) {
      Objects.requireNonNull(ip, "Ip address cannot be null");
      this.addAltName(GeneralName.iPAddress, ip);
      return this;
    }

    private Builder addAltName(int tag, String name) {
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

    public X509Certificate build()
        throws SCMSecurityException, IOException {
      Objects.requireNonNull(key, "Key cannot be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(subject),
          "Subject " + "cannot be blank");
      Preconditions.checkArgument(StringUtils.isNotBlank(clusterID),
          "Cluster ID " + "cannot be blank");
      Preconditions.checkArgument(StringUtils.isNotBlank(scmID),
          "SCM ID cannot " + "be blank");

      Preconditions.checkArgument(beginDate.isBefore(endDate), "Certificate " +
          "begin date should be before end date");

      // We just read the beginDate and EndDate as Start of the Day and
      // confirm that we do not violate the maxDuration Config.
      Duration certDuration = Duration.between(beginDate, endDate);
      Duration maxDuration = config.getMaxCertificateDuration();
      if (certDuration.compareTo(maxDuration) > 0) {
        throw new SCMSecurityException("The cert duration violates the " +
            "maximum configured value. Please check the hdds.x509.max" +
            ".duration config key. Current Value: " + certDuration +
            " config: " + maxDuration);
      }

      SelfSignedCertificate rootCertificate =
          new SelfSignedCertificate(this);
      try {
        return rootCertificate.generateCertificate(caCertSerialId);
      } catch (OperatorCreationException | CertIOException e) {
        throw new CertificateException("Unable to create root certificate.",
            e.getCause());
      }
    }
  }
}

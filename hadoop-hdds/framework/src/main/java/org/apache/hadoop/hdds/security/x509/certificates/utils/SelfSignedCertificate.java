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

package org.apache.hadoop.hdds.security.x509.certificates.utils;

import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.util.Strings;
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
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * A Self Signed Certificate with CertificateServer basic constraint can be used
 * to bootstrap a certificate infrastructure, if no external certificate is
 * provided.
 */
public final class SelfSignedCertificate {
  private static final String NAME_FORMAT = "CN=%s,OU=%s,O=%s";
  private String subject;
  private String clusterID;
  private String scmID;
  private LocalDate beginDate;
  private LocalDate endDate;
  private KeyPair key;
  private SecurityConfig config;
  private List<GeneralName> altNames;

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
    return NAME_FORMAT;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private X509CertificateHolder generateCertificate(boolean isCA)
      throws OperatorCreationException, IOException {
    // For the Root Certificate we form the name from Subject, SCM ID and
    // Cluster ID.
    String dnName = String.format(getNameFormat(), subject, scmID, clusterID);
    X500Name name = new X500Name(dnName);
    byte[] encoded = key.getPublic().getEncoded();
    SubjectPublicKeyInfo publicKeyInfo =
        SubjectPublicKeyInfo.getInstance(encoded);


    ContentSigner contentSigner =
        new JcaContentSignerBuilder(config.getSignatureAlgo())
            .setProvider(config.getProvider()).build(key.getPrivate());

    // Please note: Since this is a root certificate we use "ONE" as the
    // serial number. Also note that skip enforcing locale or UTC. We are
    // trying to operate at the Days level, hence Time zone is also skipped for
    // now.
    BigInteger serial = BigInteger.ONE;
    if (!isCA) {
      serial = new BigInteger(Long.toString(Time.monotonicNow()));
    }

    ZoneOffset zoneOffset =
        beginDate.atStartOfDay(ZoneOffset.systemDefault()).getOffset();

    // Valid from the Start of the day when we generate this Certificate.
    Date validFrom =
        Date.from(beginDate.atTime(LocalTime.MIN).toInstant(zoneOffset));

    // Valid till end day finishes.
    Date validTill =
        Date.from(endDate.atTime(LocalTime.MAX).toInstant(zoneOffset));

    X509v3CertificateBuilder builder = new X509v3CertificateBuilder(name,
        serial, validFrom, validTill, name, publicKeyInfo);

    if (isCA) {
      builder.addExtension(Extension.basicConstraints, true,
          new BasicConstraints(true));
      int keyUsageFlag = KeyUsage.keyCertSign | KeyUsage.cRLSign;
      KeyUsage keyUsage = new KeyUsage(keyUsageFlag);
      builder.addExtension(Extension.keyUsage, true, keyUsage);
      if (altNames != null && altNames.size() >= 1) {
        builder.addExtension(new Extension(Extension.subjectAlternativeName,
            false, new GeneralNames(altNames.toArray(
                new GeneralName[altNames.size()])).getEncoded()));
      }
    }
    return builder.build(contentSigner);
  }

  /**
   * Builder class for Root Certificates.
   */
  public static class Builder {
    private String subject;
    private String clusterID;
    private String scmID;
    private LocalDate beginDate;
    private LocalDate endDate;
    private KeyPair key;
    private SecurityConfig config;
    private boolean isCA;
    private List<GeneralName> altNames;

    public Builder setConfiguration(ConfigurationSource configuration) {
      this.config = new SecurityConfig(configuration);
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

    public Builder setBeginDate(LocalDate date) {
      this.beginDate = date;
      return this;
    }

    public Builder setEndDate(LocalDate date) {
      this.endDate = date;
      return this;
    }

    public Builder makeCA() {
      isCA = true;
      return this;
    }

    // Support SAN extension with DNS and RFC822 Name
    // other name type will be added as needed.
    public Builder addDnsName(String dnsName) {
      Preconditions.checkNotNull(dnsName, "dnsName cannot be null");
      this.addAltName(GeneralName.dNSName, dnsName);
      return this;
    }

    // IP address is subject to change which is optional for now.
    public Builder addIpAddress(String ip) {
      Preconditions.checkNotNull(ip, "Ip address cannot be null");
      this.addAltName(GeneralName.iPAddress, ip);
      return this;
    }

    public Builder addServiceName(
        String serviceName) {
      Preconditions.checkNotNull(
          serviceName, "Service Name cannot be null");

      this.addAltName(GeneralName.otherName, serviceName);
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

    public X509CertificateHolder build()
        throws SCMSecurityException, IOException {
      Preconditions.checkNotNull(key, "Key cannot be null");
      Preconditions.checkArgument(Strings.isNotBlank(subject), "Subject " +
          "cannot be blank");
      Preconditions.checkArgument(Strings.isNotBlank(clusterID), "Cluster ID " +
          "cannot be blank");
      Preconditions.checkArgument(Strings.isNotBlank(scmID), "SCM ID cannot " +
          "be blank");

      Preconditions.checkArgument(beginDate.isBefore(endDate), "Certificate " +
          "begin date should be before end date");

      // We just read the beginDate and EndDate as Start of the Day and
      // confirm that we do not violate the maxDuration Config.
      Duration certDuration = Duration.between(beginDate.atStartOfDay(),
          endDate.atStartOfDay());
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
        return rootCertificate.generateCertificate(isCA);
      } catch (OperatorCreationException | CertIOException e) {
        throw new CertificateException("Unable to create root certificate.",
            e.getCause());
      }
    }
  }
}

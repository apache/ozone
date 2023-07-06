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

package org.apache.hadoop.hdds.security;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Provider;
import java.security.Security;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hdds.conf.ConfigurationSource;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DEFAULT_KEY_ALGORITHM;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DEFAULT_KEY_LEN;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DEFAULT_SECURITY_PROVIDER;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_ACK_TIMEOUT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_ACK_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_CHECK_INTERNAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_CHECK_INTERNAL_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_TIME_OF_DAY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_TIME_OF_DAY_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_CERTIFICATE_FILE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_CERTIFICATE_FILE_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_CERTIFICATE_POLLING_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_CERTIFICATE_POLLING_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_PRIVATE_KEY_FILE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_PRIVATE_KEY_FILE_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_PUBLIC_KEY_FILE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_PUBLIC_KEY_FILE_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_PROVIDER;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_PROVIDER_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_TEST_CERT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_TEST_CERT_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_ALGORITHM;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_LEN;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PRIVATE_KEY_FILE_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PRIVATE_KEY_FILE_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PUBLIC_KEY_FILE_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PUBLIC_KEY_FILE_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_PROVIDER;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CRL_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CRL_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DEFAULT_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DEFAULT_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DIR_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_FILE_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_FILE_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_MAX_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_MAX_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_SIGNATURE_ALGO;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_SIGNATURE_ALGO_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;


/**
 * A class that deals with all Security related configs in HDDS.
 * <p>
 * This class allows security configs to be read and used consistently across
 * all security related code base.
 */
public class SecurityConfig {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecurityConfig.class);
  private static volatile Provider provider;
  private final int size;
  private final String keyAlgo;
  private final String providerString;
  private final String metadataDir;
  private final String keyDir;
  private final String privateKeyFileName;
  private final String publicKeyFileName;
  private final Duration maxCertDuration;
  private final String x509SignatureAlgo;
  private final boolean blockTokenEnabled;
  private final long blockTokenExpiryDurationMs;
  private final boolean tokenSanityChecksEnabled;
  private final boolean containerTokenEnabled;
  private final String certificateDir;
  private final String certificateFileName;
  private final boolean grpcTlsEnabled;
  private final Duration defaultCertDuration;
  private final Duration renewalGracePeriod;
  private final boolean isSecurityEnabled;
  private final String crlName;
  private final boolean grpcTlsUseTestCert;
  private final String externalRootCaPublicKeyPath;
  private final String externalRootCaPrivateKeyPath;
  private final String externalRootCaCert;
  private final Duration caCheckInterval;
  private final String caRotationTimeOfDay;
  private final Pattern caRotationTimeOfDayPattern =
      Pattern.compile("\\d{2}:\\d{2}:\\d{2}");
  private final Duration caAckTimeout;
  private final SslProvider grpcSSLProvider;
  private final Duration rootCaCertificatePollingInterval;

  /**
   * Constructs a SecurityConfig.
   *
   * @param configuration - HDDS Configuration
   */
  public SecurityConfig(ConfigurationSource configuration) {
    Preconditions.checkNotNull(configuration, "Configuration cannot be null");
    this.size = configuration.getInt(HDDS_KEY_LEN, HDDS_DEFAULT_KEY_LEN);
    this.keyAlgo = configuration.get(HDDS_KEY_ALGORITHM,
        HDDS_DEFAULT_KEY_ALGORITHM);
    this.providerString = configuration.get(HDDS_SECURITY_PROVIDER,
        HDDS_DEFAULT_SECURITY_PROVIDER);

    // Please Note: To make it easy for our customers we will attempt to read
    // HDDS metadata dir and if that is not set, we will use Ozone directory.
    this.metadataDir = configuration.get(HDDS_METADATA_DIR_NAME,
        configuration.get(OZONE_METADATA_DIRS));
    this.keyDir = configuration.get(HDDS_KEY_DIR_NAME,
        HDDS_KEY_DIR_NAME_DEFAULT);
    this.privateKeyFileName = configuration.get(HDDS_PRIVATE_KEY_FILE_NAME,
        HDDS_PRIVATE_KEY_FILE_NAME_DEFAULT);
    this.publicKeyFileName = configuration.get(HDDS_PUBLIC_KEY_FILE_NAME,
        HDDS_PUBLIC_KEY_FILE_NAME_DEFAULT);

    String durationString = configuration.get(HDDS_X509_MAX_DURATION,
        HDDS_X509_MAX_DURATION_DEFAULT);
    this.maxCertDuration = Duration.parse(durationString);
    this.x509SignatureAlgo = configuration.get(HDDS_X509_SIGNATURE_ALGO,
        HDDS_X509_SIGNATURE_ALGO_DEFAULT);
    this.certificateDir = configuration.get(HDDS_X509_DIR_NAME,
        HDDS_X509_DIR_NAME_DEFAULT);
    this.certificateFileName = configuration.get(HDDS_X509_FILE_NAME,
        HDDS_X509_FILE_NAME_DEFAULT);

    this.blockTokenEnabled = configuration.getBoolean(
        HDDS_BLOCK_TOKEN_ENABLED,
        HDDS_BLOCK_TOKEN_ENABLED_DEFAULT);
    this.blockTokenExpiryDurationMs = configuration.getTimeDuration(
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME,
        HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME_DEFAULT,
        TimeUnit.MILLISECONDS);
    tokenSanityChecksEnabled = configuration.getBoolean(
        HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED,
        HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED_DEFAULT);

    this.containerTokenEnabled = configuration.getBoolean(
        HDDS_CONTAINER_TOKEN_ENABLED,
        HDDS_CONTAINER_TOKEN_ENABLED_DEFAULT);

    this.grpcTlsEnabled = configuration.getBoolean(HDDS_GRPC_TLS_ENABLED,
        HDDS_GRPC_TLS_ENABLED_DEFAULT);

    if (grpcTlsEnabled) {
      this.grpcTlsUseTestCert = configuration.getBoolean(
          HDDS_GRPC_TLS_TEST_CERT, HDDS_GRPC_TLS_TEST_CERT_DEFAULT);
    } else {
      this.grpcTlsUseTestCert = false;
    }

    this.isSecurityEnabled = configuration.getBoolean(
        OZONE_SECURITY_ENABLED_KEY,
        OZONE_SECURITY_ENABLED_DEFAULT);

    String certDurationString =
        configuration.get(HDDS_X509_DEFAULT_DURATION,
            HDDS_X509_DEFAULT_DURATION_DEFAULT);
    defaultCertDuration = Duration.parse(certDurationString);
    String renewalGraceDurationString = configuration.get(
        HDDS_X509_RENEW_GRACE_DURATION,
        HDDS_X509_RENEW_GRACE_DURATION_DEFAULT);
    renewalGracePeriod = Duration.parse(renewalGraceDurationString);

    String caCheckIntervalString = configuration.get(
        HDDS_X509_CA_ROTATION_CHECK_INTERNAL,
        HDDS_X509_CA_ROTATION_CHECK_INTERNAL_DEFAULT);
    caCheckInterval = Duration.parse(caCheckIntervalString);

    String timeOfDayString = configuration.get(
        HDDS_X509_CA_ROTATION_TIME_OF_DAY,
        HDDS_X509_CA_ROTATION_TIME_OF_DAY_DEFAULT);

    Matcher matcher = caRotationTimeOfDayPattern.matcher(timeOfDayString);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Property value of " +
          HDDS_X509_CA_ROTATION_TIME_OF_DAY +
          " should follow the hh:mm:ss format.");
    }
    caRotationTimeOfDay = "1970-01-01T" + timeOfDayString;

    String ackTimeString = configuration.get(
        HDDS_X509_CA_ROTATION_ACK_TIMEOUT,
        HDDS_X509_CA_ROTATION_ACK_TIMEOUT_DEFAULT);
    caAckTimeout = Duration.parse(ackTimeString);

    validateCertificateValidityConfig();

    String rootCaCertificatePollingIntervalString = configuration.get(
        HDDS_X509_ROOTCA_CERTIFICATE_POLLING_INTERVAL,
        HDDS_X509_ROOTCA_CERTIFICATE_POLLING_INTERVAL_DEFAULT);

    this.rootCaCertificatePollingInterval =
        Duration.parse(rootCaCertificatePollingIntervalString);

    this.externalRootCaCert = configuration.get(
        HDDS_X509_ROOTCA_CERTIFICATE_FILE,
        HDDS_X509_ROOTCA_CERTIFICATE_FILE_DEFAULT);
    this.externalRootCaPublicKeyPath = configuration.get(
        HDDS_X509_ROOTCA_PUBLIC_KEY_FILE,
        HDDS_X509_ROOTCA_PUBLIC_KEY_FILE_DEFAULT);
    this.externalRootCaPrivateKeyPath = configuration.get(
        HDDS_X509_ROOTCA_PRIVATE_KEY_FILE,
        HDDS_X509_ROOTCA_PRIVATE_KEY_FILE_DEFAULT);

    this.crlName = configuration.get(HDDS_X509_CRL_NAME,
        HDDS_X509_CRL_NAME_DEFAULT);

    this.grpcSSLProvider = SslProvider.valueOf(
        configuration.get(HDDS_GRPC_TLS_PROVIDER,
            HDDS_GRPC_TLS_PROVIDER_DEFAULT));

    // First Startup -- if the provider is null, check for the provider.
    if (SecurityConfig.provider == null) {
      synchronized (SecurityConfig.class) {
        provider = Security.getProvider(this.providerString);
        if (SecurityConfig.provider == null) {
          // Provider not found, let us try to Dynamically initialize the
          // provider.
          provider = initSecurityProvider(this.providerString);
        }
      }
    }
  }

  /**
   * Check for certificate validity configuration.
   */
  private void validateCertificateValidityConfig() {
    if (maxCertDuration.isNegative() || maxCertDuration.isZero()) {
      String msg = "Property " + HDDS_X509_MAX_DURATION +
              " should not be zero or negative";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    if (defaultCertDuration.isNegative() || defaultCertDuration.isZero()) {
      String msg = "Property " + HDDS_X509_DEFAULT_DURATION +
              " should not be zero or negative";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    if (renewalGracePeriod.isNegative() || renewalGracePeriod.isZero()) {
      String msg = "Property " + HDDS_X509_RENEW_GRACE_DURATION +
              " should not be zero or negative";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    if (maxCertDuration.compareTo(defaultCertDuration) < 0) {
      String msg = "Property " + HDDS_X509_DEFAULT_DURATION +
              " should not be greater than Property " + HDDS_X509_MAX_DURATION;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    if (defaultCertDuration.compareTo(renewalGracePeriod) < 0) {
      String msg = "Property " + HDDS_X509_RENEW_GRACE_DURATION +
              " should not be greater than Property "
              + HDDS_X509_DEFAULT_DURATION;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    if (caCheckInterval.isNegative() || caCheckInterval.isZero()) {
      String msg = "Property " + HDDS_X509_CA_ROTATION_CHECK_INTERNAL +
          " should not be zero or negative";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    if (caCheckInterval.compareTo(renewalGracePeriod) >= 0) {
      throw new IllegalArgumentException("Property value of " +
          HDDS_X509_CA_ROTATION_CHECK_INTERNAL +
          " should be smaller than " + HDDS_X509_RENEW_GRACE_DURATION);
    }

    if (caAckTimeout.isNegative() || caAckTimeout.isZero()) {
      String msg = "Property " + HDDS_X509_CA_ROTATION_ACK_TIMEOUT +
          " should not be zero or negative";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    if (caAckTimeout.compareTo(renewalGracePeriod) >= 0) {
      throw new IllegalArgumentException("Property value of " +
          HDDS_X509_CA_ROTATION_ACK_TIMEOUT +
          " should be smaller than " + HDDS_X509_RENEW_GRACE_DURATION);
    }

    if (tokenSanityChecksEnabled
        && blockTokenExpiryDurationMs > renewalGracePeriod.toMillis()) {
      throw new IllegalArgumentException(" Certificate grace period " +
          HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION +
          " should be greater than maximum block/container token lifetime " +
          HddsConfigKeys.HDDS_BLOCK_TOKEN_EXPIRY_TIME);
    }
  }

  /**
   * Returns the CRL Name.
   *
   * @return String.
   */
  public String getCrlName() {
    return crlName;
  }

  /**
   * Returns true if security is enabled for OzoneCluster. This is determined by
   * value of OZONE_SECURITY_ENABLED_KEY.
   *
   * @return true if security is enabled for OzoneCluster.
   */
  public boolean isSecurityEnabled() {
    return isSecurityEnabled;
  }

  /**
   * Returns the Default Certificate Duration.
   *
   * @return Duration for the default certificate issue.
   */
  public Duration getDefaultCertDuration() {
    return defaultCertDuration;
  }

  /**
   * Duration of the grace period within which a certificate should be
   * renewed before the current one expires.
   * Default is 28 days.
   *
   * @return the value of hdds.x509.renew.grace.duration property
   */
  public Duration getRenewalGracePeriod() {
    return renewalGracePeriod;
  }

  /**
   * Returns the Standard Certificate file name.
   *
   * @return String - Name of the Certificate File.
   */
  public String getCertificateFileName() {
    return certificateFileName;
  }

  /**
   * Returns the public key file name, This is used for storing the public keys
   * on disk.
   *
   * @return String, File name used for public keys.
   */
  public String getPublicKeyFileName() {
    return publicKeyFileName;
  }

  /**
   * Returns the private key file name.This is used for storing the private keys
   * on disk.
   *
   * @return String, File name used for private keys.
   */
  public String getPrivateKeyFileName() {
    return privateKeyFileName;
  }

  /**
   * Returns the File path to where keys are stored with an additional component
   * name inserted in between.
   *
   * @param component - Component Name - String.
   * @return Path Key location.
   */
  public Path getKeyLocation(String component) {
    Preconditions.checkNotNull(this.metadataDir, "Metadata directory can't be"
        + " null. Please check configs.");
    return Paths.get(metadataDir, component, keyDir);
  }

  /**
   * Returns the File path to where certificates are stored with an addition
   * component name inserted in between.
   *
   * @param component - Component Name - String.
   * @return Path location.
   */
  public Path getCertificateLocation(String component) {
    Preconditions.checkNotNull(this.metadataDir, "Metadata directory can't be"
        + " null. Please check configs.");
    return Paths.get(metadataDir, component, certificateDir);
  }

  /**
   * Returns the File path to where this component store key and certificates.
   *
   * @param component - Component Name - String.
   * @return Path location.
   */
  public Path getLocation(String component) {
    Preconditions.checkNotNull(this.metadataDir, "Metadata directory can't be"
        + " null. Please check configs.");
    return Paths.get(metadataDir, component);
  }

  /**
   * Gets the Key Size, The default key size is 2048, since the default
   * algorithm used is RSA. User can change this by setting the "hdds.key.len"
   * in configuration.
   *
   * @return key size.
   */
  public int getSize() {
    return size;
  }

  /**
   * Returns the Provider name. SCM defaults to using Bouncy Castle and will
   * return "BC".
   *
   * @return String Provider name.
   */
  public String getProvider() {
    return providerString;
  }

  /**
   * Returns the Key generation Algorithm used.  User can change this by setting
   * the "hdds.key.algo" in configuration.
   *
   * @return String Algo.
   */
  public String getKeyAlgo() {
    return keyAlgo;
  }

  /**
   * Returns the X.509 Signature Algorithm used. This can be changed by setting
   * "hdds.x509.signature.algorithm" to the new name. The default algorithm is
   * SHA256withRSA.
   *
   * @return String
   */
  public String getSignatureAlgo() {
    return x509SignatureAlgo;
  }

  /**
   * Returns the maximum length a certificate can be valid in SCM. The default
   * value is 5 years. This can be changed by setting "hdds.x509.max.duration"
   * in configuration. The formats accepted are based on the ISO-8601 duration
   * format PnDTnHnMn.nS
   * <p>
   * Default value is 5 years and written as P1865D.
   *
   * @return Duration.
   */
  public Duration getMaxCertificateDuration() {
    return this.maxCertDuration;
  }

  /**
   * Whether to require short-lived tokens for block operations.
   */
  public boolean isBlockTokenEnabled() {
    return this.blockTokenEnabled;
  }

  public long getBlockTokenExpiryDurationMs() {
    return blockTokenExpiryDurationMs;
  }

  /**
   * Whether to require short-lived tokens for container operations.
   */
  public boolean isContainerTokenEnabled() {
    return this.containerTokenEnabled;
  }

  /**
   * Returns true if TLS is enabled for gRPC services.
   *
   * @return true if TLS is enabled for gRPC services.
   */
  public boolean isGrpcTlsEnabled() {
    return this.grpcTlsEnabled;
  }

  /**
   * Get the gRPC TLS provider.
   *
   * @return the gRPC TLS Provider.
   */
  public SslProvider getGrpcSslProvider() {
    return grpcSSLProvider;
  }

  public String getExternalRootCaPrivateKeyPath() {
    return externalRootCaPrivateKeyPath;
  }

  public String getExternalRootCaPublicKeyPath() {
    return externalRootCaPublicKeyPath;
  }

  public String getExternalRootCaCert() {
    return externalRootCaCert;
  }

  public Duration getCaCheckInterval() {
    return caCheckInterval;
  }

  public String getCaRotationTimeOfDay() {
    return caRotationTimeOfDay;
  }

  public Duration getCaAckTimeout() {
    return caAckTimeout;
  }

  public Duration getRootCaCertificatePollingInterval() {
    return rootCaCertificatePollingInterval;
  }

  /**
   * Return true if using test certificates with authority as localhost. This
   * should be used only for unit test where certificates are generated by
   * openssl with localhost as DN and should never use for production as it will
   * bypass the hostname/ip matching verification.
   *
   * @return true if using test certificates.
   */
  public boolean useTestCert() {
    return grpcTlsUseTestCert;
  }

  /**
   * Adds a security provider dynamically if it is not loaded already.
   *
   * @param providerName - name of the provider.
   */
  private Provider initSecurityProvider(String providerName) {
    if ("BC".equals(providerName)) {
      Security.addProvider(new BouncyCastleProvider());
      return Security.getProvider(providerName);
    }
    LOG.error("Security Provider:{} is unknown", provider);
    throw new SecurityException("Unknown security provider:" + provider);
  }

  public boolean isTokenEnabled() {
    return blockTokenEnabled || containerTokenEnabled;
  }
}

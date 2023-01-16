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

package org.apache.hadoop.hdds.security.x509.certificate.client;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertStore;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.ssl.KeyStoresFactory;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.hdds.security.x509.keys.SecurityUtil;
import org.apache.hadoop.ozone.OzoneSecurityUtil;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.validator.routines.DomainValidator;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.FAILURE;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.GETCERT;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.REINIT;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.SUCCESS;
import static org.apache.hadoop.hdds.security.x509.exceptions.CertificateException.ErrorCode.BOOTSTRAP_ERROR;
import static org.apache.hadoop.hdds.security.x509.exceptions.CertificateException.ErrorCode.CERTIFICATE_ERROR;
import static org.apache.hadoop.hdds.security.x509.exceptions.CertificateException.ErrorCode.CRYPTO_SIGNATURE_VERIFICATION_ERROR;
import static org.apache.hadoop.hdds.security.x509.exceptions.CertificateException.ErrorCode.CRYPTO_SIGN_ERROR;
import static org.apache.hadoop.hdds.security.x509.exceptions.CertificateException.ErrorCode.CSR_ERROR;
import static org.apache.hadoop.hdds.security.x509.exceptions.CertificateException.ErrorCode.RENEW_ERROR;
import static org.apache.hadoop.hdds.security.x509.exceptions.CertificateException.ErrorCode.ROLLBACK_ERROR;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClientWithMaxRetry;

import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;

/**
 * Default Certificate client implementation. It provides certificate
 * operations that needs to be performed by certificate clients in the Ozone
 * eco-system.
 */
public abstract class DefaultCertificateClient implements CertificateClient {

  private static final Random RANDOM = new SecureRandom();

  public static final String CERT_FILE_NAME_FORMAT = "%s.crt";
  public static final String CA_CERT_PREFIX = "CA-";
  private static final int CA_CERT_PREFIX_LEN = 3;
  public static final String ROOT_CA_CERT_PREFIX = "ROOTCA-";
  private static final int ROOT_CA_PREFIX_LEN = 7;
  private final Logger logger;
  private final SecurityConfig securityConfig;
  private final KeyCodec keyCodec;
  private PrivateKey privateKey;
  private PublicKey publicKey;
  private X509Certificate x509Certificate;
  private Map<String, X509Certificate> certificateMap;
  private String certSerialId;
  private String caCertId;
  private String rootCaCertId;
  private long localCrlId;
  private String component;
  private List<String> pemEncodedCACerts = null;
  private KeyStoresFactory serverKeyStoresFactory;
  private KeyStoresFactory clientKeyStoresFactory;

  // Lock to protect the certificate renew process, to make sure there is only
  // one renew process is ongoing at one time.
  // Certificate renew steps:
  //  1. generate new keys and sign new certificate, persist all data to disk
  //  2. switch on disk new keys and certificate with current ones
  //  3. save new certificate ID into service VERSION file
  //  4. refresh in memory certificate ID and reload all new certificates
  private Lock renewLock = new ReentrantLock();

  private ScheduledExecutorService executorService;
  private Consumer<String> certIdSaveCallback;
  private Runnable shutdownCallback;
  private SCMSecurityProtocolClientSideTranslatorPB scmSecurityProtocolClient;
  private static UserGroupInformation ugi;

  DefaultCertificateClient(SecurityConfig securityConfig, Logger log,
      String certSerialId, String component,
      Consumer<String> saveCertId, Runnable shutdown) {
    Objects.requireNonNull(securityConfig);
    this.securityConfig = securityConfig;
    keyCodec = new KeyCodec(securityConfig, component);
    this.logger = log;
    this.certificateMap = new ConcurrentHashMap<>();
    this.certSerialId = certSerialId;
    this.component = component;
    this.certIdSaveCallback = saveCertId;
    this.shutdownCallback = shutdown;

    loadAllCertificates();
  }

  public synchronized void setCertificateId(String certId) {
    Preconditions.checkArgument(certSerialId == null,
        "certSerialId should only be set once if not renew");
    this.certSerialId = certId;
    // reload all new certs
    loadAllCertificates();
  }

  /**
   * Load all certificates from configured location.
   * */
  private synchronized void loadAllCertificates() {
    // See if certs directory exists in file system.
    Path certPath = securityConfig.getCertificateLocation(component);
    if (Files.exists(certPath) && Files.isDirectory(certPath)) {
      getLogger().info("Loading certificate from location:{}.",
          certPath);
      File[] certFiles = certPath.toFile().listFiles();

      if (certFiles != null) {
        CertificateCodec certificateCodec =
            new CertificateCodec(securityConfig, component);
        long latestCaCertSerailId = -1L;
        long latestRootCaCertSerialId = -1L;
        for (File file : certFiles) {
          if (file.isFile()) {
            try {
              X509CertificateHolder x509CertificateHolder = certificateCodec
                  .readCertificate(certPath, file.getName());
              X509Certificate cert =
                  CertificateCodec.getX509Certificate(x509CertificateHolder);
              if (cert != null && cert.getSerialNumber() != null) {
                if (cert.getSerialNumber().toString().equals(certSerialId)) {
                  x509Certificate = cert;
                }
                certificateMap.putIfAbsent(cert.getSerialNumber().toString(),
                    cert);
                if (file.getName().startsWith(CA_CERT_PREFIX)) {
                  String certFileName = FilenameUtils.getBaseName(
                      file.getName());
                  long tmpCaCertSerailId = NumberUtils.toLong(
                      certFileName.substring(CA_CERT_PREFIX_LEN));
                  if (tmpCaCertSerailId > latestCaCertSerailId) {
                    latestCaCertSerailId = tmpCaCertSerailId;
                  }
                }

                if (file.getName().startsWith(ROOT_CA_CERT_PREFIX)) {
                  String certFileName = FilenameUtils.getBaseName(
                      file.getName());
                  long tmpRootCaCertSerailId = NumberUtils.toLong(
                      certFileName.substring(ROOT_CA_PREFIX_LEN));
                  if (tmpRootCaCertSerailId > latestRootCaCertSerialId) {
                    latestRootCaCertSerialId = tmpRootCaCertSerailId;
                  }
                }
                getLogger().info("Added certificate {} from file:{}.", cert,
                    file.getAbsolutePath());
              } else {
                getLogger().error("Error reading certificate from file:{}",
                    file);
              }
            } catch (java.security.cert.CertificateException | IOException e) {
              getLogger().error("Error reading certificate from file:{}.",
                  file.getAbsolutePath(), e);
            }
          }
        }
        if (latestCaCertSerailId != -1) {
          caCertId = Long.toString(latestCaCertSerailId);
        }
        if (latestRootCaCertSerialId != -1) {
          rootCaCertId = Long.toString(latestRootCaCertSerialId);
        }

        if (x509Certificate != null) {
          if (executorService == null) {
            startCertificateMonitor();
          }
        } else {
          getLogger().warn("CertificateLifetimeMonitor is not started this " +
              "time because certificate is empty.");
        }
      }
    }
  }

  /**
   * Returns the private key of the specified  if it exists on the local
   * system.
   *
   * @return private key or Null if there is no data.
   */
  @Override
  public synchronized PrivateKey getPrivateKey() {
    if (privateKey != null) {
      return privateKey;
    }

    Path keyPath = securityConfig.getKeyLocation(component);
    if (OzoneSecurityUtil.checkIfFileExist(keyPath,
        securityConfig.getPrivateKeyFileName())) {
      try {
        privateKey = keyCodec.readPrivateKey();
      } catch (InvalidKeySpecException | NoSuchAlgorithmException
          | IOException e) {
        getLogger().error("Error while getting private key.", e);
      }
    }
    return privateKey;
  }

  /**
   * Returns the public key of the specified if it exists on the local system.
   *
   * @return public key or Null if there is no data.
   */
  @Override
  public PublicKey getPublicKey() {
    if (publicKey != null) {
      return publicKey;
    }

    Path keyPath = securityConfig.getKeyLocation(component);
    if (OzoneSecurityUtil.checkIfFileExist(keyPath,
        securityConfig.getPublicKeyFileName())) {
      try {
        publicKey = keyCodec.readPublicKey();
      } catch (InvalidKeySpecException | NoSuchAlgorithmException
          | IOException e) {
        getLogger().error("Error while getting public key.", e);
      }
    }
    return publicKey;
  }

  /**
   * Returns the default certificate of given client if it exists.
   *
   * @return certificate or Null if there is no data.
   */
  @Override
  public synchronized X509Certificate getCertificate() {
    if (x509Certificate != null) {
      return x509Certificate;
    }

    if (certSerialId == null) {
      getLogger().error("Default certificate serial id is not set. Can't " +
          "locate the default certificate for this client.");
      return null;
    }
    // Refresh the cache from file system.
    loadAllCertificates();
    if (certificateMap.containsKey(certSerialId)) {
      x509Certificate = certificateMap.get(certSerialId);
    }
    return x509Certificate;
  }

  /**
   * Return the latest CA certificate known to the client.
   * @return latest ca certificate known to the client.
   */
  @Override
  public synchronized X509Certificate getCACertificate() {
    if (caCertId != null) {
      return certificateMap.get(caCertId);
    }
    return null;
  }

  /**
   * Returns the certificate  with the specified certificate serial id if it
   * exists else try to get it from SCM.
   * @param  certId
   *
   * @return certificate or Null if there is no data.
   */
  @Override
  public synchronized X509Certificate getCertificate(String certId)
      throws CertificateException {
    // Check if it is in cache.
    if (certificateMap.containsKey(certId)) {
      return certificateMap.get(certId);
    }
    // Try to get it from SCM.
    return this.getCertificateFromScm(certId);
  }

  @Override
  public List<CRLInfo> getCrls(List<Long> crlIds) throws IOException {
    try {
      return getScmSecureClient().getCrls(crlIds);
    } catch (Exception e) {
      getLogger().error("Error while getting CRL with " +
          "CRL ids:{} from scm.", crlIds, e);
      throw new CertificateException("Error while getting CRL with " +
          "CRL ids:" + crlIds, e);
    }
  }

  @Override
  public long getLatestCrlId() throws IOException {
    try {
      return getScmSecureClient().getLatestCrlId();
    } catch (Exception e) {
      getLogger().error("Error while getting latest CRL id from scm.", e);
      throw new CertificateException("Error while getting latest CRL id from" +
          " scm.", e);
    }
  }

  /**
   * Get certificate from SCM and store it in local file system.
   * @param certId
   * @return certificate
   */
  private X509Certificate getCertificateFromScm(String certId)
      throws CertificateException {

    getLogger().info("Getting certificate with certSerialId:{}.",
        certId);
    try {
      String pemEncodedCert = getScmSecureClient().getCertificate(certId);
      this.storeCertificate(pemEncodedCert, true);
      return CertificateCodec.getX509Certificate(pemEncodedCert);
    } catch (Exception e) {
      getLogger().error("Error while getting Certificate with " +
          "certSerialId:{} from scm.", certId, e);
      throw new CertificateException("Error while getting certificate for " +
          "certSerialId:" + certId, e, CERTIFICATE_ERROR);
    }
  }

  /**
   * Verifies if this certificate is part of a trusted chain.
   *
   * @param certificate - certificate.
   * @return true if it trusted, false otherwise.
   */
  @Override
  public boolean verifyCertificate(X509Certificate certificate) {
    throw new UnsupportedOperationException("Operation not supported.");
  }

  /**
   * Creates digital signature over the data stream using the s private key.
   *
   * @param stream - Data stream to sign.
   * @throws CertificateException - on Error.
   */
  @Override
  public byte[] signDataStream(InputStream stream)
      throws CertificateException {
    try {
      Signature sign = Signature.getInstance(getSignatureAlgorithm(),
          getSecurityProvider());
      sign.initSign(getPrivateKey());
      byte[] buffer = new byte[1024 * 4];

      int len;
      while (-1 != (len = stream.read(buffer))) {
        sign.update(buffer, 0, len);
      }
      return sign.sign();
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException | IOException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGN_ERROR);
    }
  }

  @Override
  public String getSecurityProvider() {
    return securityConfig.getProvider();
  }

  /**
   * Creates digital signature over the data stream using the s private key.
   *
   * @param data - Data to sign.
   * @throws CertificateException - on Error.
   */
  @Override
  public byte[] signData(byte[] data) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(getSignatureAlgorithm(),
          getSecurityProvider());

      sign.initSign(getPrivateKey());
      sign.update(data);

      return sign.sign();
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGN_ERROR);
    }
  }

  @Override
  public String getSignatureAlgorithm() {
    return securityConfig.getSignatureAlgo();
  }

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   *
   * @param stream - Data Stream.
   * @param signature - Byte Array containing the signature.
   * @param cert - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  @Override
  public boolean verifySignature(InputStream stream, byte[] signature,
      X509Certificate cert) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(getSignatureAlgorithm(),
          getSecurityProvider());
      sign.initVerify(cert);
      byte[] buffer = new byte[1024 * 4];

      int len;
      while (-1 != (len = stream.read(buffer))) {
        sign.update(buffer, 0, len);
      }
      return sign.verify(signature);
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException | IOException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGNATURE_VERIFICATION_ERROR);
    }
  }

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   *
   * @param data - Data in byte array.
   * @param signature - Byte Array containing the signature.
   * @param cert - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  @Override
  public boolean verifySignature(byte[] data, byte[] signature,
      X509Certificate cert) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(getSignatureAlgorithm(),
          getSecurityProvider());
      sign.initVerify(cert);
      sign.update(data);
      return sign.verify(signature);
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGNATURE_VERIFICATION_ERROR);
    }
  }

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   *
   * @param data - Data in byte array.
   * @param signature - Byte Array containing the signature.
   * @param pubKey - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  private boolean verifySignature(byte[] data, byte[] signature,
      PublicKey pubKey) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(getSignatureAlgorithm(),
          getSecurityProvider());
      sign.initVerify(pubKey);
      sign.update(data);
      return sign.verify(signature);
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGNATURE_VERIFICATION_ERROR);
    }
  }

  /**
   * Returns a CSR builder that can be used to creates a Certificate signing
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  @Override
  public CertificateSignRequest.Builder getCSRBuilder()
      throws CertificateException {
    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
        .setConfiguration(securityConfig.getConfiguration());
    try {
      DomainValidator validator = DomainValidator.getInstance();
      // Add all valid ips.
      OzoneSecurityUtil.getValidInetsForCurrentHost().forEach(
          ip -> {
            builder.addIpAddress(ip.getHostAddress());
            if (validator.isValid(ip.getCanonicalHostName())) {
              builder.addDnsName(ip.getCanonicalHostName());
            } else {
              getLogger().error("Invalid domain {}", ip.getCanonicalHostName());
            }
          });
    } catch (IOException e) {
      throw new CertificateException("Error while adding ip to CSR builder",
          e, CSR_ERROR);
    }
    return builder;
  }

  /**
   * Get the certificate of well-known entity from SCM.
   *
   * @param query - String Query, please see the implementation for the
   * discussion on the query formats.
   * @return X509Certificate or null if not found.
   */
  @Override
  public X509Certificate queryCertificate(String query) {
    // TODO:
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * Stores the Certificate  for this client. Don't use this api to add trusted
   * certificates of others.
   *
   * @param pemEncodedCert        - pem encoded X509 Certificate
   * @param force                 - override any existing file
   * @throws CertificateException - on Error.
   *
   */
  @Override
  public void storeCertificate(String pemEncodedCert, boolean force)
      throws CertificateException {
    this.storeCertificate(pemEncodedCert, force, false);
  }

  /**
   * Stores the Certificate  for this client. Don't use this api to add trusted
   * certificates of others.
   *
   * @param pemEncodedCert        - pem encoded X509 Certificate
   * @param force                 - override any existing file
   * @param caCert                - Is CA certificate.
   * @throws CertificateException - on Error.
   *
   */
  @Override
  public void storeCertificate(String pemEncodedCert, boolean force,
      boolean caCert) throws CertificateException {
    CertificateCodec certificateCodec = new CertificateCodec(securityConfig,
        component);
    storeCertificate(pemEncodedCert, force, caCert, false,
        certificateCodec, true);
  }

  public synchronized void storeCertificate(String pemEncodedCert,
      boolean force, boolean isCaCert, boolean isRootCaCert,
      CertificateCodec codec, boolean addToCertMap)
      throws CertificateException {
    try {
      X509Certificate cert =
          CertificateCodec.getX509Certificate(pemEncodedCert);
      String certName = String.format(CERT_FILE_NAME_FORMAT,
          cert.getSerialNumber().toString());

      if (isCaCert) {
        certName = CA_CERT_PREFIX + certName;
        caCertId = cert.getSerialNumber().toString();
      } else if (isRootCaCert) {
        certName = ROOT_CA_CERT_PREFIX + certName;
        rootCaCertId = cert.getSerialNumber().toString();
      }

      codec.writeCertificate(codec.getLocation(), certName,
          pemEncodedCert, force);
      if (addToCertMap) {
        certificateMap.putIfAbsent(cert.getSerialNumber().toString(), cert);
      }
    } catch (IOException | java.security.cert.CertificateException e) {
      throw new CertificateException("Error while storing certificate.", e,
          CERTIFICATE_ERROR);
    }
  }

  /**
   * Stores the trusted chain of certificates for a specific .
   *
   * @param ks - Key Store.
   * @throws CertificateException - on Error.
   */
  @Override
  public void storeTrustChain(CertStore ks)
      throws CertificateException {
    throw new UnsupportedOperationException("Operation not supported.");
  }


  /**
   * Stores the trusted chain of certificates for a specific .
   *
   * @param certificates - List of Certificates.
   * @throws CertificateException - on Error.
   */
  @Override
  public void storeTrustChain(List<X509Certificate> certificates)
      throws CertificateException {
    throw new UnsupportedOperationException("Operation not supported.");
  }

  /**
   * Defines 8 cases of initialization.
   * Each case specifies objects found.
   * 0. NONE                  Keypair as well as certificate not found.
   * 1. CERT                  Certificate found but keypair missing.
   * 2. PUBLIC_KEY            Public key found but private key and
   *                          certificate is missing.
   * 3. PUBLICKEY_CERT        Only public key and certificate is present.
   * 4. PRIVATE_KEY           Only private key is present.
   * 5. PRIVATEKEY_CERT       Only private key and certificate is present.
   * 6. PUBLICKEY_PRIVATEKEY  indicates private and public key were read
   *                          successfully from configured location but
   *                          Certificate.
   * 7. ALL                   Keypair as well as certificate is present.
   * 8. EXPIRED_CERT          The certificate is present, but either it has
   *                          already expired, or is about to be expired within
   *                          the grace period provided in the configuration.
   *
   * */
  protected enum InitCase {
    NONE,
    CERT,
    PUBLIC_KEY,
    PUBLICKEY_CERT,
    PRIVATE_KEY,
    PRIVATEKEY_CERT,
    PUBLICKEY_PRIVATEKEY,
    ALL,
    EXPIRED_CERT
  }

  /**
   *
   * Initializes client by performing following actions.
   * 1. Create key dir if not created already.
   * 2. Generates and stores a keypair.
   * 3. Try to recover public key if private key and certificate is present
   *    but public key is missing.
   * 4. Checks if the certificate is about to be expired or have already been
   *    expired, and if yes removes the key material and the certificate and
   *    asks for re-initialization in the result.
   *
   * Truth table:
   *  +--------------+-----------------+--------------+----------------+
   *  | Private Key  | Public Keys     | Certificate  |   Result       |
   *  +--------------+-----------------+--------------+----------------+
   *  | False  (0)   | False   (0)     | False  (0)   |   GETCERT  000 |
   *  | False  (0)   | False   (0)     | True   (1)   |   FAILURE  001 |
   *  | False  (0)   | True    (1)     | False  (0)   |   FAILURE  010 |
   *  | False  (0)   | True    (1)     | True   (1)   |   FAILURE  011 |
   *  | True   (1)   | False   (0)     | False  (0)   |   FAILURE  100 |
   *  | True   (1)   | False   (0)     | True   (1)   |   SUCCESS  101 |
   *  | True   (1)   | True    (1)     | False  (0)   |   GETCERT  110 |
   *  | True   (1)   | True    (1)     | True   (1)   |   SUCCESS  111 |
   *  +--------------+-----------------+--------------+----------------+
   *
   * @return InitResponse
   * Returns FAILURE in following cases:
   * 1. If private key is missing but public key or certificate is available.
   * 2. If public key and certificate is missing.
   *
   * Returns SUCCESS in following cases:
   * 1. If keypair as well certificate is available.
   * 2. If private key and certificate is available and public key is
   *    recovered successfully.
   *
   * Returns GETCERT in following cases:
   * 1. First time when keypair and certificate is not available, keypair
   *    will be generated and stored at configured location.
   * 2. When keypair (public/private key) is available but certificate is
   *    missing.
   *
   * Returns REINIT in following case:
   *    If it would return SUCCESS, but the certificate expiration date is
   *    within the configured grace period or if the certificate is already
   *    expired.
   *    The grace period is configured by the hdds.x509.renew.grace.duration
   *    configuration property.
   *
   */
  @Override
  public synchronized InitResponse init() throws CertificateException {
    int initCase = 0;
    PrivateKey pvtKey = getPrivateKey();
    PublicKey pubKey = getPublicKey();
    X509Certificate certificate = getCertificate();
    if (pvtKey != null) {
      initCase = initCase | 1 << 2;
    }
    if (pubKey != null) {
      initCase = initCase | 1 << 1;
    }
    if (certificate != null) {
      initCase = initCase | 1;
    }

    boolean successCase =
        initCase == InitCase.ALL.ordinal() ||
            initCase == InitCase.PRIVATEKEY_CERT.ordinal();
    boolean shouldRenew =
        certificate != null &&
            Instant.now().plus(securityConfig.getRenewalGracePeriod())
                .isAfter(certificate.getNotAfter().toInstant());

    if (successCase && shouldRenew) {
      initCase = InitCase.EXPIRED_CERT.ordinal();
    }

    getLogger().info("Certificate client init case: {}", initCase);
    Preconditions.checkArgument(initCase < InitCase.values().length, "Not a " +
        "valid case.");
    InitCase init = InitCase.values()[initCase];
    return handleCase(init);
  }

  /**
   * Default handling of each {@link InitCase}.
   * */
  protected InitResponse handleCase(InitCase init)
      throws CertificateException {
    switch (init) {
    case NONE:
      getLogger().info("Creating keypair for client as keypair and " +
          "certificate not found.");
      bootstrapClientKeys();
      return GETCERT;
    case CERT:
      getLogger().error("Private key not found, while certificate is still" +
          " present. Delete keypair and try again.");
      return FAILURE;
    case PUBLIC_KEY:
      getLogger().error("Found public key but private key and certificate " +
          "missing.");
      return FAILURE;
    case PRIVATE_KEY:
      getLogger().info("Found private key but public key and certificate " +
          "is missing.");
      // TODO: Recovering public key from private might be possible in some
      //  cases.
      return FAILURE;
    case PUBLICKEY_CERT:
      getLogger().error("Found public key and certificate but private " +
          "key is missing.");
      return FAILURE;
    case PRIVATEKEY_CERT:
      getLogger().info("Found private key and certificate but public key" +
          " missing.");
      if (recoverPublicKey()) {
        return SUCCESS;
      } else {
        getLogger().error("Public key recovery failed.");
        return FAILURE;
      }
    case PUBLICKEY_PRIVATEKEY:
      getLogger().info("Found private and public key but certificate is" +
          " missing.");
      if (validateKeyPair(getPublicKey())) {
        return GETCERT;
      } else {
        getLogger().info("Keypair validation failed.");
        return FAILURE;
      }
    case ALL:
      getLogger().info("Found certificate file along with KeyPair.");
      if (validateKeyPairAndCertificate()) {
        return SUCCESS;
      } else {
        return FAILURE;
      }
    case EXPIRED_CERT:
      getLogger().info("Component certificate is about to expire. Initiating" +
          "renewal.");
      removeMaterial();
      return REINIT;
    default:
      getLogger().error("Unexpected case: {} (private/public/cert)",
          Integer.toBinaryString(init.ordinal()));

      return FAILURE;
    }
  }

  protected void removeMaterial() throws CertificateException {
    try {
      FileUtils.deleteDirectory(
          securityConfig.getKeyLocation(component).toFile());
      getLogger().info("Certificate renewal: key material is removed.");
      FileUtils.deleteDirectory(
          securityConfig.getCertificateLocation(component).toFile());
      getLogger().info("Certificate renewal: certificates are removed.");
    } catch (IOException e) {
      throw new CertificateException("Certificate renewal failed: remove key" +
          " material failed.", e);
    }
  }

  /**
   * Validate keypair and certificate.
   * */
  protected boolean validateKeyPairAndCertificate() throws
      CertificateException {
    if (validateKeyPair(getPublicKey())) {
      getLogger().info("Keypair validated.");
      // TODO: Certificates cryptographic validity can be checked as well.
      if (validateKeyPair(getCertificate().getPublicKey())) {
        getLogger().info("Keypair validated with certificate.");
      } else {
        getLogger().error("Stored certificate is generated with different " +
            "private key.");
        return false;
      }
    } else {
      getLogger().error("Keypair validation failed.");
      return false;
    }
    return true;
  }

  /**
   * Tries to recover public key from certificate. Also validates recovered
   * public key.
   * */
  protected boolean recoverPublicKey() throws CertificateException {
    PublicKey pubKey = getCertificate().getPublicKey();
    try {

      if (validateKeyPair(pubKey)) {
        keyCodec.writePublicKey(pubKey);
        publicKey = pubKey;
      } else {
        getLogger().error("Can't recover public key " +
            "corresponding to private key.");
        return false;
      }
    } catch (IOException e) {
      throw new CertificateException("Error while trying to recover " +
          "public key.", e, BOOTSTRAP_ERROR);
    }
    return true;
  }

  /**
   * Validates public and private key of certificate client.
   *
   * @param pubKey
   * */
  protected boolean validateKeyPair(PublicKey pubKey)
      throws CertificateException {
    byte[] challenge =
        RandomStringUtils.random(1000, 0, 0, false, false, null, RANDOM)
            .getBytes(StandardCharsets.UTF_8);
    byte[]  sign = signDataStream(new ByteArrayInputStream(challenge));
    return verifySignature(challenge, sign, pubKey);
  }

  /**
   * Bootstrap the client by creating keypair and storing it in configured
   * location.
   * */
  protected void bootstrapClientKeys() throws CertificateException {
    Path keyPath = securityConfig.getKeyLocation(component);
    if (Files.notExists(keyPath)) {
      try {
        Files.createDirectories(keyPath);
      } catch (IOException e) {
        throw new CertificateException("Error while creating directories " +
            "for certificate storage.", BOOTSTRAP_ERROR);
      }
    }
    KeyPair keyPair = createKeyPair(keyCodec);
    privateKey = keyPair.getPrivate();
    publicKey = keyPair.getPublic();
  }

  protected KeyPair createKeyPair(KeyCodec codec) throws CertificateException {
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = null;
    try {
      keyPair = keyGenerator.generateKey();
      codec.writePublicKey(keyPair.getPublic());
      codec.writePrivateKey(keyPair.getPrivate());
    } catch (NoSuchProviderException | NoSuchAlgorithmException
        | IOException e) {
      getLogger().error("Error while bootstrapping certificate client.", e);
      throw new CertificateException("Error while bootstrapping certificate.",
          BOOTSTRAP_ERROR);
    }
    return keyPair;
  }

  public Logger getLogger() {
    return logger;
  }

  public String getComponentName() {
    return component;
  }

  @Override
  public synchronized X509Certificate getRootCACertificate() {
    if (rootCaCertId != null) {
      return certificateMap.get(rootCaCertId);
    }
    return null;
  }

  @Override
  public void storeRootCACertificate(String pemEncodedCert, boolean force)
      throws CertificateException {
    CertificateCodec certificateCodec = new CertificateCodec(securityConfig,
        component);
    storeCertificate(pemEncodedCert, force, false, true,
        certificateCodec, true);
  }

  @Override
  public synchronized List<String> getCAList() {
    return pemEncodedCACerts;
  }

  @Override
  public synchronized List<String> listCA() throws IOException {
    if (pemEncodedCACerts == null) {
      updateCAList();
    }
    return pemEncodedCACerts;
  }

  @Override
  public synchronized List<String> updateCAList() throws IOException {
    try {
      pemEncodedCACerts = getScmSecureClient().listCACertificate();
      return pemEncodedCACerts;
    } catch (Exception e) {
      getLogger().error("Error during updating CA list", e);
      throw new CertificateException("Error during updating CA list", e,
          CERTIFICATE_ERROR);
    }
  }

  @Override
  public boolean processCrl(CRLInfo crl) {
    List<String> certIds2Remove = new ArrayList();
    crl.getX509CRL().getRevokedCertificates().forEach(
        cert -> certIds2Remove.add(cert.getSerialNumber().toString()));
    boolean reinitCert = removeCertificates(certIds2Remove);
    setLocalCrlId(crl.getCrlSequenceID());
    return reinitCert;
  }

  private synchronized boolean removeCertificates(List<String> certIds) {
    boolean reInitCert = false;

    // For now, remove self cert and ca cert is not implemented
    // both requires a restart of the service.
    if ((certSerialId != null && certIds.contains(certSerialId)) ||
        (caCertId != null && certIds.contains(caCertId)) ||
        (rootCaCertId != null && certIds.contains(rootCaCertId))) {
      reInitCert = true;
    }

    Path basePath = securityConfig.getCertificateLocation(component);
    for (String certId : certIds) {
      if (certificateMap.containsKey(certId)) {
        // remove on disk
        String certName = String.format(CERT_FILE_NAME_FORMAT, certId);

        if (certId.equals(caCertId)) {
          certName = CA_CERT_PREFIX + certName;
        }

        if (certId.equals(rootCaCertId)) {
          certName = ROOT_CA_CERT_PREFIX + certName;
        }

        FileUtils.deleteQuietly(basePath.resolve(certName).toFile());
        // remove in memory
        certificateMap.remove(certId);

        // TODO: reset certSerialId, caCertId or rootCaCertId
      }
    }
    return reInitCert;
  }

  public long getLocalCrlId() {
    return this.localCrlId;
  }

  /**
   * Set Local CRL id.
   * @param crlId
   */
  public void setLocalCrlId(long crlId) {
    this.localCrlId = crlId;
  }

  @Override
  public synchronized KeyStoresFactory getServerKeyStoresFactory()
      throws CertificateException {
    if (serverKeyStoresFactory == null) {
      serverKeyStoresFactory = SecurityUtil.getServerKeyStoresFactory(
          securityConfig, this, true);
    }
    return serverKeyStoresFactory;
  }

  @Override
  public KeyStoresFactory getClientKeyStoresFactory()
      throws CertificateException {
    if (clientKeyStoresFactory == null) {
      clientKeyStoresFactory = SecurityUtil.getClientKeyStoresFactory(
          securityConfig, this, true);
    }
    return clientKeyStoresFactory;
  }

  @Override
  public synchronized void close() throws IOException {
    if (executorService != null) {
      executorService.shutdown();
    }

    if (serverKeyStoresFactory != null) {
      serverKeyStoresFactory.destroy();
    }

    if (clientKeyStoresFactory != null) {
      clientKeyStoresFactory.destroy();
    }
  }

  /**
   * Check how much time before certificate will enter expiry grace period.
   * @return Duration, time before certificate enters the grace
   *                   period defined by "hdds.x509.renew.grace.duration"
   */
  public Duration timeBeforeExpiryGracePeriod(X509Certificate certificate) {
    Duration gracePeriod = securityConfig.getRenewalGracePeriod();
    Date expireDate = certificate.getNotAfter();
    LocalDateTime gracePeriodStart = expireDate.toInstant()
        .atZone(ZoneId.systemDefault()).toLocalDateTime().minus(gracePeriod);
    LocalDateTime currentTime = LocalDateTime.now();
    if (gracePeriodStart.isBefore(currentTime)) {
      // Cert is already in grace period time.
      return Duration.ZERO;
    } else {
      return Duration.between(currentTime, gracePeriodStart);
    }
  }

  /**
   * Renew keys and certificate. Save the keys are certificate to disk in new
   * directories, swap the current key directory and certs directory with the
   * new directories.
   * @param force, check certificate expiry time again if force is false.
   * @return String, new certificate ID
   * */
  public String renewAndStoreKeyAndCertificate(boolean force)
      throws CertificateException {
    if (!force) {
      synchronized (this) {
        Preconditions.checkArgument(
            timeBeforeExpiryGracePeriod(x509Certificate).isZero());
      }
    }

    String newKeyPath = securityConfig.getKeyLocation(component)
        .toString() + HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX;
    String newCertPath = securityConfig.getCertificateLocation(component)
        .toString() + HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX;
    File newKeyDir = new File(newKeyPath);
    File newCertDir = new File(newCertPath);
    try {
      FileUtils.deleteDirectory(newKeyDir);
      FileUtils.deleteDirectory(newCertDir);
      Files.createDirectories(newKeyDir.toPath());
      Files.createDirectories(newCertDir.toPath());
    } catch (IOException e) {
      throw new CertificateException("Error while deleting/creating " +
          newKeyPath + " or " + newCertPath + " directories to cleanup " +
          " certificate storage. ", e, RENEW_ERROR);
    }

    // Generate key
    KeyCodec newKeyCodec = new KeyCodec(securityConfig, newKeyDir.toPath());
    KeyPair newKeyPair;
    try {
      newKeyPair = createKeyPair(newKeyCodec);
    } catch (CertificateException e) {
      throw new CertificateException("Error while creating new key pair.",
          e, RENEW_ERROR);
    }

    // Get certificate signed
    String newCertSerialId;
    try {
      CertificateSignRequest.Builder csrBuilder = getCSRBuilder(newKeyPair);
      newCertSerialId = signAndStoreCertificate(csrBuilder.build(),
          Paths.get(newCertPath));
    } catch (Exception e) {
      throw new CertificateException("Error while signing and storing new" +
          " certificates.", e, RENEW_ERROR);
    }

    // switch Key and Certs directory on disk
    File currentKeyDir = new File(
        securityConfig.getKeyLocation(component).toString());
    File currentCertDir = new File(
        securityConfig.getCertificateLocation(component).toString());
    File backupKeyDir = new File(
        securityConfig.getKeyLocation(component).toString() +
            HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX);
    File backupCertDir = new File(
        securityConfig.getCertificateLocation(component).toString() +
            HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX);

    try {
      Files.move(currentKeyDir.toPath(), backupKeyDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      // Cannot move current key dir to the backup dir
      throw new CertificateException("Failed to move " +
          currentKeyDir.getAbsolutePath() +
          " to " + backupKeyDir.getAbsolutePath() + " during " +
          "certificate renew.", RENEW_ERROR);
    }

    try {
      Files.move(currentCertDir.toPath(), backupCertDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      // Cannot move current cert dir to the backup dir
      rollbackBackupDir(currentKeyDir, currentCertDir, backupKeyDir,
          backupCertDir);
      throw new CertificateException("Failed to move " +
          currentCertDir.getAbsolutePath() +
          " to " + backupCertDir.getAbsolutePath() + " during " +
          "certificate renew.", RENEW_ERROR);
    }

    try {
      Files.move(newKeyDir.toPath(), currentKeyDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      // Cannot move new dir as the current dir
      String msg = "Failed to move " + newKeyDir.getAbsolutePath() +
          " to " + currentKeyDir.getAbsolutePath() +
          " during certificate renew.";
      // rollback
      rollbackBackupDir(currentKeyDir, currentCertDir, backupKeyDir,
          backupCertDir);
      throw new CertificateException(msg, RENEW_ERROR);
    }

    try {
      Files.move(newCertDir.toPath(), currentCertDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      // Cannot move new dir as the current dir
      String msg = "Failed to move " + newCertDir.getAbsolutePath() +
          " to " + currentCertDir.getAbsolutePath() +
          " during certificate renew.";
      // delete currentKeyDir which is moved from new key directory
      try {
        FileUtils.deleteDirectory(new File(currentKeyDir.toString()));
      } catch (IOException e1) {
        getLogger().error("Failed to delete current KeyDir {} which is moved " +
            " from the newly generated KeyDir {}", currentKeyDir, newKeyDir, e);
        throw new CertificateException(msg, RENEW_ERROR);
      }
      // rollback
      rollbackBackupDir(currentKeyDir, currentCertDir, backupKeyDir,
          backupCertDir);
      throw new CertificateException(msg, RENEW_ERROR);
    }

    getLogger().info("Successful renew key and certificate." +
        " New certificate {}.", newCertSerialId);
    return newCertSerialId;
  }

  private void rollbackBackupDir(File currentKeyDir, File currentCertDir,
      File backupKeyDir, File backupCertDir) throws CertificateException {
    // move backup dir back as current dir
    try {
      Files.move(backupKeyDir.toPath(), currentKeyDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      String msg = "Failed to move " + backupKeyDir.getAbsolutePath() +
          " back to " + currentKeyDir.getAbsolutePath() +
          " during rollback.";
      // Need a manual recover process.
      throw new CertificateException(msg, ROLLBACK_ERROR);
    }

    try {
      Files.move(backupCertDir.toPath(), currentCertDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      String msg = "Failed to move " + backupCertDir.getAbsolutePath() +
          " back to " + currentCertDir.getAbsolutePath()  +
          " during rollback.";
      // Need a manual recover process.
      throw new CertificateException(msg, ROLLBACK_ERROR);
    }

    Preconditions.checkArgument(currentCertDir.exists());
    Preconditions.checkArgument(currentKeyDir.exists());
  }

  /**
   * Delete old backup key and cert directory.
   */
  public void cleanBackupDir() {
    File backupKeyDir = new File(
        securityConfig.getKeyLocation(component).toString() +
            HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX);
    File backupCertDir = new File(
        securityConfig.getCertificateLocation(component).toString() +
            HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX);
    if (backupKeyDir.exists()) {
      try {
        FileUtils.deleteDirectory(backupKeyDir);
      } catch (IOException e) {
        getLogger().error("Error while deleting {} directories for " +
            "certificate storage cleanup.", backupKeyDir, e);
      }
    }
    if (backupCertDir.exists()) {
      try {
        FileUtils.deleteDirectory(backupCertDir);
      } catch (IOException e) {
        getLogger().error("Error while deleting {} directories for " +
            "certificate storage cleanup.", backupCertDir, e);
      }
    }
  }

  synchronized void reloadKeyAndCertificate(String newCertId) {
    // reset current value
    privateKey = null;
    publicKey = null;
    x509Certificate = null;
    certSerialId = null;
    caCertId = null;
    rootCaCertId = null;

    setCertificateId(newCertId);
    getLogger().info("Reset and reload key and all certificates.");
  }

  public SecurityConfig getSecurityConfig() {
    return securityConfig;
  }

  public OzoneConfiguration getConfig() {
    return (OzoneConfiguration)securityConfig.getConfiguration();
  }

  @Override
  public abstract String signAndStoreCertificate(
      PKCS10CertificationRequest request, Path certPath)
      throws CertificateException;

  public String signAndStoreCertificate(PKCS10CertificationRequest request)
      throws CertificateException {
    return signAndStoreCertificate(request,
        getSecurityConfig().getCertificateLocation(getComponentName()));
  }

  @Override
  public abstract CertificateSignRequest.Builder getCSRBuilder(KeyPair keyPair)
      throws CertificateException;

  public SCMSecurityProtocolClientSideTranslatorPB getScmSecureClient()
      throws IOException {
    if (scmSecurityProtocolClient == null) {
      scmSecurityProtocolClient =
          getScmSecurityClientWithMaxRetry(
              (OzoneConfiguration) securityConfig.getConfiguration(), ugi);
    }
    return scmSecurityProtocolClient;
  }

  @VisibleForTesting
  public void setSecureScmClient(
      SCMSecurityProtocolClientSideTranslatorPB client) {
    scmSecurityProtocolClient = client;
  }

  @VisibleForTesting
  public static void setUgi(UserGroupInformation user) {
    ugi = user;
  }

  public synchronized void startCertificateMonitor() {
    Preconditions.checkNotNull(getCertificate(),
        "Component certificate should not be empty");
    // Schedule task to refresh certificate before it expires
    Duration gracePeriod = securityConfig.getRenewalGracePeriod();
    long timeBeforeGracePeriod =
        timeBeforeExpiryGracePeriod(x509Certificate).toMillis();
    // At least three chances to renew the certificate before it expires
    long interval =
        Math.min(gracePeriod.toMillis() / 3, TimeUnit.DAYS.toMillis(1));

    if (executorService == null) {
      executorService = Executors.newScheduledThreadPool(1,
          new ThreadFactoryBuilder().setNameFormat("CertificateLifetimeMonitor")
              .setDaemon(true).build());
    }
    this.executorService.scheduleAtFixedRate(new CertificateLifetimeMonitor(),
        timeBeforeGracePeriod, interval, TimeUnit.MILLISECONDS);
    getLogger().info("CertificateLifetimeMonitor for {} is started with " +
            "first delay {} ms and interval {} ms.", component,
        timeBeforeGracePeriod, interval);
  }

  /**
   *  Task to monitor certificate lifetime and renew the certificate if needed.
   */
  public class CertificateLifetimeMonitor implements Runnable {
    @Override
    public void run() {

      renewLock.lock();
      try {
        Duration timeLeft = timeBeforeExpiryGracePeriod(getCertificate());
        if (timeLeft.isZero()) {
          String newCertId;
          try {
            getLogger().info("Current certificate has entered the expiry" +
                    " grace period {}. Starting renew key and certs.",
                timeLeft, securityConfig.getRenewalGracePeriod());
            newCertId = renewAndStoreKeyAndCertificate(false);
          } catch (CertificateException e) {
            if (e.errorCode() ==
                CertificateException.ErrorCode.ROLLBACK_ERROR) {
              if (shutdownCallback != null) {
                getLogger().error("Failed to rollback key and cert after an " +
                    " unsuccessful renew try.", e);
                shutdownCallback.run();
              }
            }
            getLogger().error("Failed to renew and store key and cert." +
                " Keep using existing certificates.", e);
            return;
          }

          // Persist new cert serial id in component VERSION file
          if (certIdSaveCallback != null) {
            certIdSaveCallback.accept(newCertId);
          }

          // reset and reload all certs
          reloadKeyAndCertificate(newCertId);
          // cleanup backup directory
          cleanBackupDir();
        }
      } finally {
        renewLock.unlock();
      }
    }
  }
}

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

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.PKIProfile;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.UNABLE_TO_ISSUE_CERTIFICATE;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.CERTIFICATE_ERROR;

/**
 * The default CertificateServer used by SCM. This has no dependencies on any
 * external system, this allows us to bootstrap a CertificateServer from
 * Scratch.
 * <p>
 * Details =======
 * <p>
 * The Default CA server is one of the many possible implementations of an SCM
 * Certificate Authority.
 * <p>
 * A certificate authority needs the Root Certificates and its private key to
 * operate.  The init function of the DefaultCA Server detects four possible
 * states the System can be in.
 * <p>
 * 1.  Success - This means that the expected Certificates and Keys are in
 * place, and the CA was able to read those files into memory.
 * <p>
 * 2. Missing Keys - This means that private keys are missing. This is an error
 * state which SCM CA cannot recover from. The cluster might have been
 * initialized earlier and for some reason, we are not able to find the private
 * keys for the CA. Eventually we will have 2 ways to recover from this state,
 * first one is to copy the SCM CA private keys from a backup. Second one is to
 * rekey the whole cluster. Both of these are improvements we will support in
 * future.
 * <p>
 * 3. Missing Certificate - Similar to Missing Keys, but the root certificates
 * are missing.
 * <p>
 * 4. Initialize - We don't have keys or certificates. DefaultCA assumes that
 * this is a system bootup and will generate the keys and certificates
 * automatically.
 * <p>
 * The init() follows the following logic,
 * <p>
 * 1. Compute the Verification Status -- Success, Missing Keys, Missing Certs or
 * Initialize.
 * <p>
 * 2. ProcessVerificationStatus - Returns a Lambda, based on the Verification
 * Status.
 * <p>
 * 3. Invoke the Lambda function.
 * <p>
 * At the end of the init function, we have functional CA. This function can be
 * invoked as many times since we will regenerate the keys and certs only if
 * both of them are missing.
 */
public abstract class DefaultCAServer implements CertificateServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultCAServer.class);
  private static final String CERT_FILE_EXTENSION = ".crt";
  private static final String CERT_FILE_NAME_FORMAT = "%s" + CERT_FILE_EXTENSION;
  private final String subject;
  private final String clusterID;
  private final String scmID;
  private final String componentName;
  private SecurityConfig config;
  /**
   * TODO: We will make these configurable in the future.
   */
  private PKIProfile profile;
  private CertificateApprover approver;
  private CertificateStore store;
  private Lock lock;
  private Consumer<String> saveCertId;

  /**
   * Create an Instance of DefaultCAServer.
   *
   * @param subject          - String Subject
   * @param clusterID        - String ClusterID
   * @param scmID            - String SCMID.
   * @param certificateStore - A store used to persist Certificates.
   */
  @SuppressWarnings("parameternumber")
  public DefaultCAServer(String subject, String clusterID, String scmID,
      CertificateStore certificateStore, PKIProfile pkiProfile, String componentName, Consumer<String> saveCertId) {
    this.subject = subject;
    this.clusterID = clusterID;
    this.scmID = scmID;
    this.store = certificateStore;
    this.profile = pkiProfile;
    this.componentName = componentName;
    this.saveCertId = saveCertId;
    lock = new ReentrantLock();
  }

  @Override
  public void init(SecurityConfig securityConfig)
      throws IOException {
    this.config = securityConfig;
    this.approver = new DefaultApprover(profile, this.config);
    verifySelfSignedCA();
  }

  @Override
  public X509Certificate getCACertificate() throws IOException {
    CertificateCodec certificateCodec = new CertificateCodec(config, componentName);
    try {
      return certificateCodec.getTargetCert();
    } catch (CertificateException e) {
      throw new IOException(e);
    }
  }

  @Override
  public CertPath getCaCertPath()
      throws CertificateException, IOException {
    CertificateCodec codec = new CertificateCodec(config, componentName);
    return codec.getCertPath();
  }

  /**
   * Returns the Certificate corresponding to given certificate serial id if
   * exist. Return null if it doesn't exist.
   *
   * @param certSerialId         - Certificate for this CA.
   * @return X509Certificate
   * @throws IOException - on Error.
   */
  @Override
  public X509Certificate getCertificate(String certSerialId) throws IOException {
    return store.getCertificateByID(new BigInteger(certSerialId));
  }

  private PrivateKey getPrivateKey() throws IOException {
    KeyStorage keyStorage = new KeyStorage(config, componentName);
    return keyStorage.readPrivateKey();
  }

  @Override
  public Future<CertPath> requestCertificate(
      String csr,
      CertificateApprover.ApprovalType approverType, NodeType role,
      String certSerialId) {
    LocalDateTime beginDate = LocalDateTime.now();
    LocalDateTime endDate = expiryFor(beginDate, role);

    CompletableFuture<Void> csrInspection = approver.inspectCSR(csr);
    CompletableFuture<CertPath> certPathPromise = new CompletableFuture<>();
    if (csrInspection.isCompletedExceptionally()) {
      try {
        csrInspection.get();
      } catch (Exception e) {
        certPathPromise.completeExceptionally(new SCMSecurityException("Failed to verify the CSR.", e));
      }
    }
    try {
      switch (approverType) {
      case MANUAL:
        certPathPromise.completeExceptionally(new SCMSecurityException("Manual approval is not yet implemented."));
        break;
      case KERBEROS_TRUSTED:
      case TESTING_AUTOMATIC:
        X509Certificate signedCertificate = signAndStoreCertificate(beginDate, endDate, csr, role, certSerialId);
        CertificateCodec codec = new CertificateCodec(config, componentName);
        CertPath certPath = codec.getCertPath();
        CertPath updatedCertPath = codec.prependCertToCertPath(signedCertificate, certPath);
        certPathPromise.complete(updatedCertPath);
        break;
      default:
        return null; // cannot happen, keeping checkstyle happy.
      }
    } catch (CertificateException | IOException e) {
      LOG.error("Unable to issue a certificate.", e);
      certPathPromise.completeExceptionally(new SCMSecurityException(e, UNABLE_TO_ISSUE_CERTIFICATE));
    }
    return certPathPromise;
  }

  private LocalDateTime expiryFor(LocalDateTime beginDate, NodeType role) {
    // When issuing certificates for sub-ca use the max certificate duration similar to self-signed root certificate.
    if (role == NodeType.SCM) {
      return beginDate.plus(config.getMaxCertificateDuration());
    }
    return beginDate.plus(config.getDefaultCertDuration());
  }

  private X509Certificate signAndStoreCertificate(
      LocalDateTime beginDate, LocalDateTime endDate, String csr, NodeType role, String certSerialId
  ) throws IOException, CertificateException {

    lock.lock();
    X509Certificate xcert;
    try {
      Preconditions.checkState(!Strings.isNullOrEmpty(certSerialId));
      xcert = approver.sign(config,
          getPrivateKey(),
          getCACertificate(),
          Date.from(beginDate.atZone(ZoneId.systemDefault()).toInstant()),
          Date.from(endDate.atZone(ZoneId.systemDefault()).toInstant()),
          csr, scmID, clusterID, certSerialId);
      if (store != null) {
        store.checkValidCertID(xcert.getSerialNumber());
        store.storeValidCertificate(xcert.getSerialNumber(), xcert, role);
      }
    } finally {
      lock.unlock();
    }
    return xcert;
  }

  @Override
  public List<X509Certificate> listCertificate(NodeType role,
      long startSerialId, int count) throws IOException {
    return store.listCertificate(role, BigInteger.valueOf(startSerialId), count);
  }

  @Override
  public void reinitialize(SCMMetadataStore scmMetadataStore) {
    store.reinitialize(scmMetadataStore);
  }

  /**
   * Verify Self-Signed CertificateServer. 1. Check if the Certificate exist. 2.
   * Check if the key pair exists.
   *
   * @return true if certificates and keys are present, false if all of them are missing
   * @throws IllegalStateException at least one key or certificate is present but not all of them
   */
  boolean verifySelfSignedCA() {
    /*
    The following is the truth table for the States.
    True means we have that file False means it is missing.
    +--------------+--------+--------+--------------+
    | Certificates |  Keys  | Result |   Function   |
    +--------------+--------+--------+--------------+
    | True         | True   | True   | Success      |
    | False        | False  | True   | Initialize   |
    | True         | False  | False  | Missing Key  |
    | False        | True   | False  | Missing Cert |
    +--------------+--------+--------+--------------+
    */

    boolean keyStatus = checkIfKeysExist();
    boolean certStatus = checkIfCertificatesExist(config.getCertificateLocation(componentName));

    if (certStatus && keyStatus) {
      LOG.info("CertificateServer validation is successful");
      return true;
    }

    if (!certStatus && !keyStatus) {
      initKeysAndCa();
      return false;
    }

    if (certStatus) {
      LOG.error("We have found the Certificate for this CertificateServer, " +
          "but keys used by this CertificateServer is missing. This is a non-recoverable error. " +
          "Please restart the system after locating the Keys used by the CertificateServer.");
      LOG.error("Exiting due to unrecoverable CertificateServer error.");
      throw new IllegalStateException("Missing Keys, cannot continue.");
    }

    if (!certStatus && keyStatus && config.useExternalCACertificate(componentName)) {
      initKeysAndCa();
      return false;
    }

    LOG.error("We found the keys, but the root certificate for this CertificateServer is missing. " +
        "Please restart SCM after locating the Certificates.");
    LOG.error("Exiting due to unrecoverable CertificateServer error.");
    throw new IllegalStateException("Missing CA Certs, cannot continue.");
  }

  abstract void initKeysAndCa();

  /**
   * Returns Keys status.
   *
   * @return True if the key files exist.
   */
  private boolean checkIfKeysExist() {
    KeyStorage storage = null;
    try {
      storage = new KeyStorage(config, componentName);
      storage.readKeyPair();
    } catch (IOException e) {
      if (storage != null && config.useExternalCACertificate(componentName)) {
        try {
          storage.readPrivateKey();
          return true;
        } catch (IOException ignored) { }
      }
      return false;
    }
    return true;
  }

  /**
   * Returns certificate Status.
   *
   * @return True if the Certificate files exist.
   */
  private boolean checkIfCertificatesExist(Path rootCACertPath) {
    if (!Files.exists(rootCACertPath)) {
      return false;
    }
    return Files.exists(Paths.get(rootCACertPath.toString(),
        this.config.getCertificateFileName()));
  }

  /**
   * Generates a KeyPair for the Certificate.
   *
   * @param securityConfig - SecurityConfig.
   * @return Key Pair.
   * @throws NoSuchProviderException  - on Error.
   * @throws NoSuchAlgorithmException - on Error.
   * @throws IOException              - on Error.
   */
  KeyPair generateKeys(SecurityConfig securityConfig)
      throws NoSuchProviderException, NoSuchAlgorithmException, IOException {
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(securityConfig);
    KeyPair keys = keyGenerator.generateKey();
    KeyStorage keyStorage = new KeyStorage(securityConfig, componentName);
    keyStorage.storeKeyPair(keys);
    return keys;
  }

  public synchronized void storeCertificate(String pemEncodedCert,
      CAType caType) throws org.apache.hadoop.hdds.security.x509.exception.CertificateException {
    try {
      CertificateCodec codec = new CertificateCodec(getSecurityConfig(), getComponentName());
      CertPath certificatePath =
          CertificateCodec.getCertPathFromPemEncodedString(pemEncodedCert);
      X509Certificate cert = (X509Certificate) certificatePath.getCertificates().get(0);
      String certName = String.format(CERT_FILE_NAME_FORMAT,
          caType.getFileNamePrefix() + cert.getSerialNumber().toString());
      codec.writeCertificate(certName, pemEncodedCert);
    } catch (IOException | java.security.cert.CertificateException e) {
      throw new org.apache.hadoop.hdds.security.x509.exception.CertificateException("Error while storing certificate.",
          e,
          CERTIFICATE_ERROR);
    }
  }

  public SecurityConfig getSecurityConfig() {
    return config;
  }

  public String getComponentName() {
    return componentName;
  }

  public Consumer<String> getSaveCertId() {
    return saveCertId;
  }

  public String getSubject() {
    return subject;
  }

  public String getClusterID() {
    return clusterID;
  }

  public String getScmID() {
    return scmID;
  }
}

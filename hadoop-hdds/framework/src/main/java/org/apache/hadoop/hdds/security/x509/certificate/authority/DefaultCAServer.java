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

import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.UNABLE_TO_ISSUE_CERTIFICATE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.PKIProfile;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class DefaultCAServer implements CertificateServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultCAServer.class);
  private final String subject;
  private final String clusterID;
  private final String scmID;
  private String componentName;
  private Path caRootX509Path;
  private SecurityConfig config;
  /**
   * TODO: We will make these configurable in the future.
   */
  private PKIProfile profile;
  private CertificateApprover approver;
  private CertificateStore store;
  private Lock lock;
  private BigInteger rootCertificateId;

  /**
   * Create an Instance of DefaultCAServer.
   *  @param subject - String Subject
   * @param clusterID - String ClusterID
   * @param scmID - String SCMID.
   * @param certificateStore - A store used to persist Certificates.
   */
  public DefaultCAServer(String subject, String clusterID, String scmID,
      CertificateStore certificateStore, BigInteger rootCertId,
      PKIProfile pkiProfile, String componentName) {
    this.subject = subject;
    this.clusterID = clusterID;
    this.scmID = scmID;
    this.store = certificateStore;
    this.rootCertificateId = rootCertId;
    this.profile = pkiProfile;
    this.componentName = componentName;
    lock = new ReentrantLock();
  }

  public DefaultCAServer(String subject, String clusterID, String scmID,
      CertificateStore certificateStore, PKIProfile pkiProfile,
      String componentName) {
    this(subject, clusterID, scmID, certificateStore, BigInteger.ONE,
        pkiProfile, componentName);
  }

  @Override
  public void init(SecurityConfig securityConfig, CAType type)
      throws IOException {
    caRootX509Path = securityConfig.getCertificateLocation(componentName);
    this.config = securityConfig;
    this.approver = new DefaultApprover(profile, this.config);

    /* In future we will split this code to have different kind of CAs.
     * Right now, we have only self-signed CertificateServer.
     */

    VerificationStatus status = verifySelfSignedCA(securityConfig);
    Consumer<SecurityConfig> caInitializer =
        processVerificationStatus(status, type);
    caInitializer.accept(securityConfig);
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
      PKCS10CertificationRequest csr,
      CertificateApprover.ApprovalType approverType, NodeType role,
      String certSerialId) {
    Duration certDuration = getDuration(role);

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
        X509Certificate signedCertificate = signAndStoreCertificate(certDuration, csr, role, certSerialId);
        CertificateCodec codec = new CertificateCodec(config, componentName);
        CertPath certPath = codec.getCertPath();
        CertPath updatedCertPath = codec.prependCertToCertPath(signedCertificate, certPath);
        certPathPromise.complete(updatedCertPath);
        break;
      default:
        return null; // cannot happen, keeping checkstyle happy.
      }
    } catch (CertificateException | IOException | OperatorCreationException e) {
      LOG.error("Unable to issue a certificate.", e);
      certPathPromise.completeExceptionally(new SCMSecurityException(e, UNABLE_TO_ISSUE_CERTIFICATE));
    }
    return certPathPromise;
  }

  private Duration getDuration(NodeType role) {
    // When issuing certificates for sub-ca use the max certificate duration similar to self-signed root certificate.
    if (role == NodeType.SCM) {
      return config.getMaxCertificateDuration();
    }
    return config.getDefaultCertDuration();
  }

  private X509Certificate signAndStoreCertificate(
      Duration duration, PKCS10CertificationRequest csr, NodeType role, String certSerialId
  ) throws IOException, OperatorCreationException, CertificateException {

    ZonedDateTime beginDate = ZonedDateTime.now();
    ZonedDateTime endDate = beginDate.plus(duration);

    lock.lock();
    X509Certificate xcert;
    try {
      Preconditions.checkState(!Strings.isNullOrEmpty(certSerialId));
      xcert = approver.sign(config,
          getPrivateKey(),
          getCACertificate(),
          Date.from(beginDate.toInstant()),
          Date.from(endDate.toInstant()),
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
   * Generates a Self Signed CertificateServer. These are the steps in
   * generating a Self-Signed CertificateServer.
   * <p>
   * 1. Generate a Private/Public Key Pair. 2. Persist to a protected location.
   * 3. Generate a SelfSigned Root CertificateServer certificate.
   *
   * @param securityConfig - Config.
   */
  private void generateSelfSignedCA(SecurityConfig securityConfig) throws
      NoSuchAlgorithmException, NoSuchProviderException, IOException {
    KeyPair keyPair = generateKeys(securityConfig);
    generateRootCertificate(securityConfig, keyPair);
  }

  /**
   * Verify Self-Signed CertificateServer. 1. Check if the Certificate exist. 2.
   * Check if the key pair exists.
   *
   * @param securityConfig -- Config
   * @return Verification Status
   */
  private VerificationStatus verifySelfSignedCA(SecurityConfig securityConfig) {
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

    This truth table maps to ~(certs xor keys) or certs == keys
     */
    boolean keyStatus = checkIfKeysExist();
    boolean certStatus = checkIfCertificatesExist();

    // Check if both certStatus and keyStatus is set to true and return success.
    if ((certStatus == keyStatus) && (certStatus)) {
      return VerificationStatus.SUCCESS;
    }

    // At this point both certStatus and keyStatus should be false if they
    // are equal
    if ((certStatus == keyStatus)) {
      return VerificationStatus.INITIALIZE;
    }

    // At this point certStatus is not equal to keyStatus.
    if (certStatus) {
      return VerificationStatus.MISSING_KEYS;
    }

    return VerificationStatus.MISSING_CERTIFICATE;
  }

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
  private boolean checkIfCertificatesExist() {
    if (!Files.exists(caRootX509Path)) {
      return false;
    }
    return Files.exists(Paths.get(caRootX509Path.toString(),
        this.config.getCertificateFileName()));
  }

  /**
   * Based on the Status of the verification, we return a lambda that gets
   * executed by the init function of the CA.
   *
   * @param status - Verification Status.
   */
  @VisibleForTesting
  Consumer<SecurityConfig> processVerificationStatus(
      VerificationStatus status,  CAType type) {
    Consumer<SecurityConfig> consumer = null;
    switch (status) {
    case SUCCESS:
      consumer = (arg) -> LOG.info("CertificateServer validation is " +
          "successful");
      break;
    case MISSING_KEYS:
      consumer = (arg) -> {
        LOG.error("We have found the Certificate for this CertificateServer, " +
            "but keys used by this CertificateServer is missing. This is a " +
            "non-recoverable error. Please restart the system after locating " +
            "the Keys used by the CertificateServer.");
        LOG.error("Exiting due to unrecoverable CertificateServer error.");
        throw new IllegalStateException("Missing Keys, cannot continue.");
      };
      break;
    case MISSING_CERTIFICATE:
      if (config.useExternalCACertificate(componentName) && type == CAType.ROOT) {
        consumer = this::initRootCa;
      } else {
        consumer = (arg) -> {
          LOG.error("We found the keys, but the root certificate for this " +
              "CertificateServer is missing. Please restart SCM after locating " +
              "the " +
              "Certificates.");
          LOG.error("Exiting due to unrecoverable CertificateServer error.");
          throw new IllegalStateException("Missing Root Certs, cannot continue.");
        };
      }
      break;
    case INITIALIZE:
      if (type == CAType.ROOT) {
        consumer = this::initRootCa;
      } else if (type == CAType.SUBORDINATE) {
        // For sub CA certificates are generated during bootstrap/init. If
        // both keys/certs are missing, init/bootstrap is missed to be
        // performed.
        consumer = (arg) -> {
          LOG.error("Sub SCM CA Server is missing keys/certs. SCM is started " +
              "with out init/bootstrap");
          throw new IllegalStateException("INTERMEDIARY_CA Should not be" +
              " in Initialize State during startup.");
        };
      }
      break;
    default:
      /* Make CheckStyle happy */
      break;
    }
    return consumer;
  }

  private void initRootCa(SecurityConfig securityConfig) {
    if (securityConfig.useExternalCACertificate(componentName)) {
      initWithExternalRootCa(securityConfig);
    } else {
      try {
        generateSelfSignedCA(securityConfig);
      } catch (NoSuchProviderException | NoSuchAlgorithmException
               | IOException e) {
        LOG.error("Unable to initialize CertificateServer.", e);
      }
    }
    VerificationStatus newStatus = verifySelfSignedCA(securityConfig);
    if (newStatus != VerificationStatus.SUCCESS) {
      LOG.error("Unable to initialize CertificateServer, failed in " +
          "verification.");
    }
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
  private KeyPair generateKeys(SecurityConfig securityConfig)
      throws NoSuchProviderException, NoSuchAlgorithmException, IOException {
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(securityConfig);
    KeyPair keys = keyGenerator.generateKey();
    KeyStorage keyStorage = new KeyStorage(securityConfig, componentName);
    keyStorage.storeKeyPair(keys);
    return keys;
  }

  /**
   * Generates a self-signed Root Certificate for CA.
   *
   * @param securityConfig - SecurityConfig
   * @param key            - KeyPair.
   * @throws IOException          - on Error.
   * @throws SCMSecurityException - on Error.
   */
  private void generateRootCertificate(
      SecurityConfig securityConfig, KeyPair key)
      throws IOException, SCMSecurityException {
    Objects.requireNonNull(this.config, "this.config == null");
    ZonedDateTime beginDate = ZonedDateTime.now();
    ZonedDateTime endDate = beginDate.plus(securityConfig.getMaxCertificateDuration());
    SelfSignedCertificate.Builder builder = SelfSignedCertificate.newBuilder()
        .setSubject(this.subject)
        .setScmID(this.scmID)
        .setClusterID(this.clusterID)
        .setBeginDate(beginDate)
        .setEndDate(endDate)
        .makeCA(rootCertificateId)
        .setConfiguration(securityConfig)
        .setKey(key);

    builder.addInetAddresses();
    X509Certificate selfSignedCertificate = builder.build();

    CertificateCodec certCodec =
        new CertificateCodec(config, componentName);
    certCodec.writeCertificate(selfSignedCertificate);
  }

  private void initWithExternalRootCa(SecurityConfig conf) {
    Path extCertPath = Paths.get(conf.getExternalRootCaCert());

    try {
      CertificateCodec certificateCodec = new CertificateCodec(config, componentName);
      Path extCertParent = extCertPath.getParent();
      Path extCertName = extCertPath.getFileName();
      if (extCertParent == null || extCertName == null) {
        throw new IOException("External cert path is not correct: " +
            extCertPath);
      }
      X509Certificate certificate = certificateCodec.getTargetCert(extCertParent, extCertName.toString());

      certificateCodec.writeCertificate(certificate);
    } catch (IOException | CertificateException  e) {
      LOG.error("External root CA certificate initialization failed", e);
    }
  }

  /**
   * This represents the verification status of the CA. Based on this enum
   * appropriate action is taken in the Init.
   */
  @VisibleForTesting
  enum VerificationStatus {
    SUCCESS, /* All artifacts needed by CertificateServer is present */
    MISSING_KEYS, /* Private key is missing, certificate Exists.*/
    MISSING_CERTIFICATE, /* Keys exist, but root certificate missing.*/
    INITIALIZE /* All artifacts are missing, we should init the system. */
  }

}

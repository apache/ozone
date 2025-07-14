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

package org.apache.hadoop.hdds.scm.security;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NEW_KEY_CERT_DIR_NAME_PROGRESS_SUFFIX;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DIR_NAME_DEFAULT;
import static org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.CERTIFICATE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.CertInfoProto;
import org.apache.hadoop.hdds.scm.ha.HASecurityUtils;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMServiceException;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.ha.StatefulService;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.CertInfo;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Root CA Rotation Service is a service in SCM to control the CA rotation.
 */
public class RootCARotationManager extends StatefulService {

  private static final Logger LOG =
      LoggerFactory.getLogger(RootCARotationManager.class);

  private static final String SERVICE_NAME =
      RootCARotationManager.class.getSimpleName();

  private final StorageContainerManager scm;
  private final OzoneConfiguration ozoneConf;
  private final SecurityConfig secConf;
  private final SCMContext scmContext;
  private final ScheduledExecutorService executorService;
  private final Duration checkInterval;
  private final Duration renewalGracePeriod;
  private final Date timeOfDay;
  private final Duration ackTimeout;
  private final Duration rootCertPollInterval;
  private final SCMCertificateClient scmCertClient;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final AtomicBoolean isProcessing = new AtomicBoolean(false);
  private final AtomicReference<Long> processStartTime =
      new AtomicReference<>();
  private final AtomicBoolean isPostProcessing = new AtomicBoolean(false);
  private final String threadName;
  private final String newCAComponent = SCM_ROOT_CA_COMPONENT_NAME +
      HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX +
      HDDS_NEW_KEY_CERT_DIR_NAME_PROGRESS_SUFFIX;

  private RootCARotationHandler handler;
  private final SequenceIdGenerator sequenceIdGen;
  private ScheduledFuture rotationTask;
  private ScheduledFuture waitAckTask;
  private ScheduledFuture waitAckTimeoutTask;
  private final RootCARotationMetrics metrics;
  private ScheduledFuture clearPostProcessingTask;

  /**
   * Constructs RootCARotationManager with the specified arguments.
   *
   * @param scm the storage container manager
   *
   * <pre>
   * {@code
   *                         (1)   (3)(4)
   *                   --------------------------->
   *                         (2)                        scm2(Follower)
   *    (1) (3)(4)     <---------------------------
   *     -------      |
   *    |        \   |
   *     ----->  scm1(Leader)
   *    \  (2)  |     \
   *     ------->      \      (1)  (3)(4)
   *                   --------------------------->
   *                          (2)                       scm3(Follower)
   *                   <---------------------------
   * }
   * </pre>
   *   (1) Rotation Prepare
   *   (2) Rotation Prepare Ack
   *   (3) Rotation Commit
   *   (4) Rotation Committed
   */
  public RootCARotationManager(StorageContainerManager scm) {
    super(scm.getStatefulServiceStateManager());
    this.scm = scm;
    this.ozoneConf = scm.getConfiguration();
    this.secConf = new SecurityConfig(ozoneConf);
    this.scmContext = scm.getScmContext();

    checkInterval = secConf.getCaCheckInterval();
    ackTimeout = secConf.getCaAckTimeout();
    renewalGracePeriod = secConf.getRenewalGracePeriod();
    timeOfDay = Date.from(LocalDateTime.parse(secConf.getCaRotationTimeOfDay())
        .atZone(ZoneId.systemDefault()).toInstant());
    rootCertPollInterval = secConf.getRootCaCertificatePollingInterval();

    threadName = scm.threadNamePrefix() + SERVICE_NAME;
    executorService = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat(threadName)
            .setDaemon(true).build());

    scmCertClient = (SCMCertificateClient) scm.getScmCertificateClient();
    sequenceIdGen = scm.getSequenceIdGen();
    handler = new RootCARotationHandlerImpl.Builder()
        .setRatisServer(scm.getScmHAManager().getRatisServer())
        .setStorageContainerManager(scm)
        .setRootCARotationManager(this)
        .build();
    scm.getSCMServiceManager().register(this);
    metrics = RootCARotationMetrics.create();
  }

  /**
   * Receives a notification for raft or safe mode related status changes.
   * Stops monitor task if it's running and the current SCM becomes a
   * follower or enter safe mode. Starts monitor if the current SCM becomes
   * leader and not in safe mode.
   */
  @Override
  public void notifyStatusChanged() {
    // stop the monitor task
    if (!scmContext.isLeader() || scmContext.isInSafeMode()) {
      if (isRunning.compareAndSet(true, false)) {
        LOG.info("notifyStatusChanged: disable monitor task.");
        if (rotationTask != null) {
          rotationTask.cancel(true);
        }
        if (waitAckTask != null) {
          waitAckTask.cancel(true);
        }
        if (waitAckTimeoutTask != null) {
          waitAckTimeoutTask.cancel(true);
        }
        if (clearPostProcessingTask != null) {
          clearPostProcessingTask.cancel(true);
        }
        isProcessing.set(false);
        processStartTime.set(null);
        isPostProcessing.set(false);
      }
      return;
    }

    if (isRunning.compareAndSet(false, true)) {
      LOG.info("notifyStatusChanged: enable monitor task");
      // enable post rotation task if needed.
      try {
        checkAndHandlePostProcessing();
      } catch (IOException | CertificateException e) {
        throw new RuntimeException(
            "Error while checking post-processing state.", e);
      }
    }
    return;
  }

  /**
   * Checks if monitor task should start (after a leader change, restart
   * etc.) by reading persisted state.
   * @return true if the persisted state is true, otherwise false
   */
  @Override
  public boolean shouldRun() {
    return true;
  }

  /**
   * @return Name of this service.
   */
  @Override
  public String getServiceName() {
    return RootCARotationManager.class.getSimpleName();
  }

  /**
   * Schedule monitor task.
   */
  @Override
  public void start() throws SCMServiceException {
    executorService.scheduleAtFixedRate(
        new MonitorTask(scmCertClient, scm.getScmStorageConfig()),
        0, checkInterval.toMillis(), TimeUnit.MILLISECONDS);
    LOG.info("Monitor task for root certificate {} is started with " +
        "interval {}.", scmCertClient.getCACertificate().getSerialNumber(),
        checkInterval);
    executorService.scheduleAtFixedRate(this::removeExpiredCertTask, 0,
        secConf.getExpiredCertificateCheckInterval().toMillis(),
        TimeUnit.MILLISECONDS);
    LOG.info("Scheduling expired certificate removal with interval {}s",
        secConf.getExpiredCertificateCheckInterval().getSeconds());
  }

  private void removeExpiredCertTask() {
    if (!isRunning.get()) {
      return;
    }
    if (scm.getCertificateStore() != null) {
      try {
        scm.getCertificateStore().removeAllExpiredCertificates();
      } catch (IOException e) {
        LOG.error("Failed to remove some expired certificates", e);
      }
    }
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public void scheduleSubCaRotationPrepareTask(String rootCertId) {
    executorService.schedule(new SubCARotationPrepareTask(rootCertId), 0,
        TimeUnit.MILLISECONDS);
  }

  public boolean isRotationInProgress() {
    return isProcessing.get();
  }

  public boolean isPostRotationInProgress() {
    return isPostProcessing.get();
  }

  /**
   *  Task to monitor certificate lifetime and start rotation if needed.
   */
  public class MonitorTask implements Runnable {
    private SCMCertificateClient certClient;
    private SCMStorageConfig scmStorageConfig;

    public MonitorTask(SCMCertificateClient client,
                       SCMStorageConfig storageConfig) {
      this.certClient = client;
      this.scmStorageConfig = storageConfig;
    }

    @Override
    public void run() {
      Thread.currentThread().setName(threadName +
          (isRunning() ? "-Active" : "-Inactive"));
      if (!isRunning.get()) {
        return;
      }
      // Lock to protect the root CA certificate rotation process,
      // to make sure there is only one task is ongoing at one time.
      synchronized (RootCARotationManager.class) {
        if (isProcessing.get()) {
          LOG.info("Root certificate rotation task is already running.");
          return;
        }
        try {
          X509Certificate rootCACert = certClient.getCACertificate();
          Duration timeLeft = timeBefore2ExpiryGracePeriod(rootCACert);
          if (timeLeft.isZero()) {
            LOG.info("Root certificate {} has entered the 2 * expiry" +
                    " grace period({}).",
                rootCACert.getSerialNumber().toString(), renewalGracePeriod);
            // schedule root CA rotation task
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime timeToSchedule = LocalDateTime.of(
                now.getYear(), now.getMonthValue(), now.getDayOfMonth(),
                timeOfDay.getHours(), timeOfDay.getMinutes(),
                timeOfDay.getSeconds());
            if (timeToSchedule.isBefore(now)) {
              timeToSchedule = timeToSchedule.plusDays(1);
            }
            long delay = Duration.between(now, timeToSchedule).toMillis();
            if (timeToSchedule.isAfter(rootCACert.getNotAfter().toInstant()
                .atZone(ZoneId.systemDefault()).toLocalDateTime())) {
              LOG.info("Configured rotation time {} is after root" +
                      " certificate {} end time {}. Start the rotation " +
                      "immediately.", timeToSchedule,
                  rootCACert.getSerialNumber().toString(),
                  rootCACert.getNotAfter());
              delay = 0;
            }

            rotationTask = executorService.schedule(
                new RotationTask(certClient, scmStorageConfig), delay,
                TimeUnit.MILLISECONDS);
            isProcessing.set(true);
            metrics.incrTotalRotationNum();
            LOG.info("Root certificate {} rotation task is scheduled with {} ms"
                + " delay", rootCACert.getSerialNumber().toString(), delay);
          }
        } catch (Throwable e) {
          LOG.error("Error while scheduling root CA rotation task", e);
          scm.shutDown("Error while scheduling root CA rotation task");
        }
      }
    }
  }

  /**
   *  Task to rotate root certificate.
   */
  public class RotationTask implements Runnable {
    private SCMCertificateClient certClient;
    private SCMStorageConfig scmStorageConfig;

    public RotationTask(SCMCertificateClient client,
        SCMStorageConfig storageConfig) {
      this.certClient = client;
      this.scmStorageConfig = storageConfig;
    }

    @Override
    public void run() {
      if (!isRunning.get()) {
        isProcessing.set(false);
        processStartTime.set(null);
        return;
      }
      // Lock to protect the root CA certificate rotation process,
      // to make sure there is only one task is ongoing at one time.
      // Root CA rotation steps:
      //  1. generate new root CA keys and certificate, persist to disk
      //  2. start new Root CA server
      //  3. send scm Sub-CA rotation preparation request through RATIS
      //  4. wait for all SCM to ack
      //  5. send scm Sub-CA rotation commit request through RATIS
      //  6. send scm Sub-CA rotation finish request through RATIS
      synchronized (RootCARotationManager.class) {
        X509Certificate rootCACert = certClient.getCACertificate();
        Duration timeLeft = timeBefore2ExpiryGracePeriod(rootCACert);
        if (timeLeft.isZero()) {
          LOG.info("Root certificate {} rotation is started.",
              rootCACert.getSerialNumber().toString());
          processStartTime.set(System.nanoTime());
          // generate new root key pair and persist new root certificate
          CertificateServer newRootCAServer = null;
          BigInteger newId = BigInteger.ONE;
          try {
            newId = new BigInteger(String.valueOf(
                sequenceIdGen.getNextId(CERTIFICATE_ID)));
            newRootCAServer =
                HASecurityUtils.initializeRootCertificateServer(secConf,
                    scm.getCertificateStore(), scmStorageConfig, newId,
                    new DefaultCAProfile(), newCAComponent);
          } catch (Throwable e) {
            LOG.error("Error while generating new root CA certificate " +
                "under {}", newCAComponent, e);
            String message = "Terminate SCM, encounter exception(" +
                e.getMessage() + ") when generating new root CA certificate " +
                "under " + newCAComponent;
            cleanupAndStop(message);
            scm.shutDown(message);
          }

          String newRootCertId = "";
          X509Certificate newRootCertificate;
          try {
            // prevent findbugs false alert
            if (newRootCAServer == null) {
              throw new Exception("New root CA server should not be null");
            }
            newRootCertificate = newRootCAServer.getCACertificate();
            newRootCertId = newRootCertificate.getSerialNumber().toString();
            Preconditions.checkState(newRootCertId.equals(newId.toString()),
                "Root certificate doesn't match, " +
                "expected:" + newId + ", fetched:" + newRootCertId);
            scm.getSecurityProtocolServer()
                .setRootCertificateServer(newRootCAServer);


            if (isRunning()) {
              checkInterruptState();
              handler.rotationPrepare(newRootCertId);
              LOG.info("Send out sub CA rotation prepare request for new " +
                  "root certificate {}", newRootCertId);
            } else {
              LOG.info("SCM is not leader anymore. Delete the in-progress " +
                      "root CA directory");
              cleanupAndStop("SCM is not leader anymore");
              return;
            }
          } catch (Exception e) {
            LOG.error("Error while sending rotation prepare request", e);
            cleanupAndStop("Error while sending rotation prepare request");
            return;
          }

          // schedule task to wait for prepare acks
          waitAckTask = executorService.scheduleAtFixedRate(
              new WaitSubCARotationPrepareAckTask(newRootCertificate),
              1, 1, TimeUnit.SECONDS);
          waitAckTimeoutTask = executorService.schedule(() -> {
            // No enough acks are received
            waitAckTask.cancel(true);
            String msg = "Failed to receive all acks of rotation prepare" +
                " after " + ackTimeout + ", received " +
                handler.rotationPrepareAcks() + " acks";
            cleanupAndStop(msg);
          }, ackTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } else {
          LOG.warn("Root certificate {} hasn't entered the 2 * expiry" +
                  " grace period {}. Skip root certificate rotation this time.",
              rootCACert.getSerialNumber().toString(), renewalGracePeriod);
          isProcessing.set(false);
          processStartTime.set(null);
        }
      }
    }
  }

  private void checkInterruptState() {
    // check whether thread is interrupted(cancelled) before
    // time-consuming ratis request
    if (Thread.currentThread().isInterrupted()) {
      cleanupAndStop(this.getClass().getSimpleName() +
          " is interrupted");
      return;
    }
  }

  private void cleanupAndStop(String reason) {
    try {
      scm.getSecurityProtocolServer().setRootCertificateServer(null);

      FileUtils.deleteDirectory(new File(scmCertClient.getSecurityConfig()
          .getLocation(newCAComponent).toString()));
      LOG.info("In-progress root CA directory {} is deleted for '{}'",
          scmCertClient.getSecurityConfig().getLocation(newCAComponent),
          reason);
    } catch (IOException ex) {
      LOG.error("Error when deleting in-progress root CA directory {} for {}",
          scmCertClient.getSecurityConfig().getLocation(newCAComponent), reason,
          ex);
    }
    isProcessing.set(false);
    processStartTime.set(null);
  }

  /**
   * Calculate time before root certificate will enter 2 * expiry grace period.
   * @return Duration, time before certificate enters the 2 * grace
   *                   period defined by "hdds.x509.renew.grace.duration"
   */
  public Duration timeBefore2ExpiryGracePeriod(X509Certificate certificate) {
    LocalDateTime gracePeriodStart = certificate.getNotAfter().toInstant()
        .minus(renewalGracePeriod).minus(renewalGracePeriod)
        .atZone(ZoneId.systemDefault()).toLocalDateTime();
    LocalDateTime currentTime = LocalDateTime.now();
    if (gracePeriodStart.isBefore(currentTime)) {
      // Cert is already in grace period time.
      return Duration.ZERO;
    } else {
      return Duration.between(currentTime, gracePeriodStart);
    }
  }

  /**
   *  Task to generate sub-ca key and certificate.
   */
  public class SubCARotationPrepareTask implements Runnable {
    private String rootCACertId;

    public SubCARotationPrepareTask(String newRootCertId) {
      this.rootCACertId = newRootCertId;
    }

    @Override
    public void run() {
      // Lock to protect the sub CA certificate rotation preparation process,
      // to make sure there is only one task is ongoing at one time.
      // Sub CA rotation preparation steps:
      //  1. generate new sub CA keys
      //  2. send CSR to leader SCM
      //  3. wait CSR response and persist the certificate to disk
      synchronized (RootCARotationManager.class) {
        try {
          LOG.info("SubCARotationPrepareTask[rootCertId = {}] - started.",
              rootCACertId);

          if (shouldSkipRootCert(rootCACertId)) {
            // Send ack to rotationPrepare request
            sendRotationPrepareAck(rootCACertId,
                scmCertClient.getCertificate().getSerialNumber().toString());
            return;
          }

          SecurityConfig securityConfig =
              scmCertClient.getSecurityConfig();
          String progressComponent = SCMCertificateClient.COMPONENT_NAME +
              HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX +
              HDDS_NEW_KEY_CERT_DIR_NAME_PROGRESS_SUFFIX;
          final String newSubCAProgressPath =
              securityConfig.getLocation(progressComponent).toString();
          final String newSubCAPath = securityConfig.getLocation(
              SCMCertificateClient.COMPONENT_NAME +
                  HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX).toString();

          File newProgressDir = new File(newSubCAProgressPath);
          File newDir = new File(newSubCAPath);
          try {
            FileUtils.deleteDirectory(newProgressDir);
            FileUtils.deleteDirectory(newDir);
            Files.createDirectories(newProgressDir.toPath());
          } catch (IOException e) {
            LOG.error("Failed to delete and create {}, or delete {}",
                newProgressDir, newDir, e);
            String message = "Terminate SCM, encounter IO exception(" +
                e.getMessage() + ") when deleting and create directory";
            scm.shutDown(message);
          }

          // Generate key
          KeyStorage keyStorage = new KeyStorage(securityConfig, progressComponent);
          KeyPair newKeyPair = null;
          try {
            HDDSKeyGenerator keyGenerator =
                new HDDSKeyGenerator(securityConfig);
            newKeyPair = keyGenerator.generateKey();
            keyStorage.storeKeyPair(newKeyPair);
            LOG.info("SubCARotationPrepareTask[rootCertId = {}] - " +
                "scm key generated.", rootCACertId);
          } catch (Exception e) {
            LOG.error("Failed to generate key under {}", newProgressDir, e);
            String message = "Terminate SCM, encounter exception(" +
                e.getMessage() + ") when generating new key under " +
                newProgressDir;
            scm.shutDown(message);
          }

          checkInterruptState();
          // Get certificate signed
          String newCertSerialId = "";
          try {
            CertificateSignRequest.Builder csrBuilder =
                scmCertClient.configureCSRBuilder();
            csrBuilder.setKey(newKeyPair);
            newCertSerialId = scmCertClient.signAndStoreCertificate(
                csrBuilder.build(),
                Paths.get(newSubCAProgressPath, HDDS_X509_DIR_NAME_DEFAULT),
                true);
            LOG.info("SubCARotationPrepareTask[rootCertId = {}] - " +
                "scm certificate {} signed.", rootCACertId, newCertSerialId);
          } catch (Exception e) {
            LOG.error("Failed to generate certificate under {}",
                newProgressDir, e);
            String message = "Terminate SCM, encounter exception(" +
                e.getMessage() + ") when generating new certificate " +
                newProgressDir;
            scm.shutDown(message);
          }

          // move dir from *-next-progress to *-next
          try {
            Files.move(newProgressDir.toPath(), newDir.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
          } catch (IOException e) {
            LOG.error("Failed to move {} to {}",
                newSubCAProgressPath, newSubCAPath, e);
            String message = "Terminate SCM, encounter exception(" +
                e.getMessage() + ") when moving " + newSubCAProgressPath +
                " to " + newSubCAPath;
            scm.shutDown(message);
          }

          // Send ack to rotationPrepare request
          checkInterruptState();
          sendRotationPrepareAck(rootCACertId, newCertSerialId);
        } catch (Throwable e) {
          LOG.error("Unexpected error happen", e);
          scm.shutDown("Unexpected error happen, " + e.getMessage());
        }
      }
    }
  }

  private void sendRotationPrepareAck(String newRootCACertId,
      String newSubCACertId) {
    // Send ack to rotationPrepare request
    try {
      handler.setSubCACertId(newSubCACertId);
      handler.rotationPrepareAck(newRootCACertId, newSubCACertId,
          scm.getScmId());
      LOG.info("SubCARotationPrepareTask[rootCertId = {}] - " +
              "rotation prepare ack sent out, new scm certificate {}",
          newRootCACertId, newSubCACertId);
    } catch (Exception e) {
      LOG.error("Failed to send ack to rotationPrepare request", e);
      String message = "Terminate SCM, encounter exception(" +
          e.getMessage() + ") when sending out rotationPrepare ack";
      scm.shutDown(message);
    }
  }

  /**
   *  Task to wait the all acks of prepare request.
   */
  public class WaitSubCARotationPrepareAckTask implements Runnable {
    private String rootCACertId;
    private X509Certificate rootCACertificate;

    public WaitSubCARotationPrepareAckTask(X509Certificate rootCACertificate) {
      this.rootCACertificate = rootCACertificate;
      this.rootCACertId = rootCACertificate.getSerialNumber().toString();
    }

    @Override
    public void run() {
      checkInterruptState();
      if (!isRunning()) {
        LOG.info("SCM is not leader anymore. Delete the in-progress " +
            "root CA directory");
        cleanupAndStop("SCM is not leader anymore");
        return;
      }

      synchronized (RootCARotationManager.class) {
        int numFromHADetails =
            scm.getSCMHANodeDetails().getPeerNodeDetails().size() + 1;
        int numFromRatisServer = scm.getScmHAManager().getRatisServer()
            .getDivision().getRaftConf().getCurrentPeers().size();
        LOG.info("numFromHADetails {}, numFromRatisServer {}",
            numFromHADetails, numFromRatisServer);
        if (handler.rotationPrepareAcks() == numFromRatisServer) {
          // all acks are received.
          try {
            waitAckTimeoutTask.cancel(true);
            handler.rotationCommit(rootCACertId);
            handler.rotationCommitted(rootCACertId);

            metrics.incrSuccessRotationNum();
            long timeTaken = System.nanoTime() - processStartTime.get();
            metrics.setSuccessTimeInNs(timeTaken);
            processStartTime.set(null);

            try {
              if (scm.getCertificateStore().getCertificateByID(
                  rootCACertificate.getSerialNumber()) == null) {
                LOG.info("Persist root certificate {} to cert store",
                    rootCACertId);
                scm.getCertificateStore().storeValidCertificate(
                    rootCACertificate.getSerialNumber(), rootCACertificate,
                    HddsProtos.NodeType.SCM);
              }
            } catch (IOException e) {
              LOG.error("Failed to save root certificate {} to cert store",
                  rootCACertId);
              scm.shutDown("Failed to save root certificate to cert store");
            }

            // reset state
            handler.resetRotationPrepareAcks();
            String msg = "Root certificate " + rootCACertId +
                " rotation is finished successfully after " + timeTaken + " ns";
            cleanupAndStop(msg);

            // set the isPostProcessing to true, which will block the CSR
            // signing in this period.
            enterPostProcessing(rootCertPollInterval.toMillis());
            // save the new root certificate to rocksdb through ratis
            saveConfiguration(
                new CertInfo.Builder()
                    .setX509Certificate(rootCACertificate)
                    .setTimestamp(rootCACertificate.getNotBefore().getTime())
                    .build()
                    .getProtobuf()
            );
          } catch (Throwable e) {
            LOG.error("Execution error", e);
            handler.resetRotationPrepareAcks();
            cleanupAndStop("Execution error, " + e.getMessage());
          } finally {
            waitAckTask.cancel(true);
          }
        }
      }
    }
  }

  private void enterPostProcessing(long delay) {
    isPostProcessing.set(true);
    LOG.info("isPostProcessing is true for {} ms", delay);
    clearPostProcessingTask = executorService.schedule(() -> {
      isPostProcessing.set(false);
      LOG.info("isPostProcessing is false");
      try {
        deleteConfiguration();
        LOG.info("Stateful configuration is deleted");
      } catch (IOException e) {
        LOG.error("Failed to delete stateful configuration", e);
      }
    }, delay, TimeUnit.MILLISECONDS);
  }

  /**
   * Stops scheduled monitor task.
   */
  @Override
  public void stop() {
    if (metrics != null) {
      metrics.unRegister();
    }

    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  @VisibleForTesting
  public void setRootCARotationHandler(RootCARotationHandler newHandler) {
    handler = newHandler;
  }

  public boolean shouldSkipRootCert(String newRootCertId) throws IOException {
    List<X509Certificate> scmCertChain = scmCertClient.getTrustChain();
    Preconditions.checkArgument(scmCertChain.size() > 1);
    X509Certificate rootCert = scmCertChain.get(scmCertChain.size() - 1);
    if (rootCert.getSerialNumber().compareTo(new BigInteger(newRootCertId))
        >= 0) {
      // usually this will happen when reapply RAFT log during SCM start
      LOG.info("Sub CA certificate {} is already signed by root " +
              "certificate {} or a newer root certificate.",
          scmCertChain.get(0).getSerialNumber().toString(), newRootCertId);
      return true;
    }
    return false;
  }

  private void checkAndHandlePostProcessing() throws IOException,
      CertificateException {
    CertInfoProto proto = readConfiguration(CertInfoProto.class);
    if (proto == null) {
      LOG.info("No {} configuration found in stateful storage",
          getServiceName());
      return;
    }

    X509Certificate cert =
        CertificateCodec.getX509Certificate(proto.getX509Certificate());

    List<X509Certificate> scmCertChain = scmCertClient.getTrustChain();
    Preconditions.checkArgument(scmCertChain.size() > 1);
    X509Certificate rootCert = scmCertChain.get(scmCertChain.size() - 1);

    int result = rootCert.getSerialNumber().compareTo(cert.getSerialNumber());
    if (result > 0) {
      // this could happen if the previous stateful configuration is not deleted
      LOG.warn("Root CA certificate ID {} in stateful storage is smaller than" +
              " current scm's root certificate ID {}", cert.getSerialNumber(),
          rootCert.getSerialNumber());

      deleteConfiguration();
      LOG.warn("Stateful configuration is deleted");
      return;
    } else if (result < 0) {
      // this should not happen
      throw new RuntimeException("Root CA certificate ID " +
          cert.getSerialNumber() + " in stateful storage is bigger than " +
          "current scm's root CA certificate ID " + rootCert.getSerialNumber());
    }

    Date issueTime = rootCert.getNotBefore();
    Date now = Calendar.getInstance().getTime();
    Duration gap = Duration.between(issueTime.toInstant(), now.toInstant());
    gap = gap.minus(rootCertPollInterval);
    if (gap.isNegative()) {
      long delay = -gap.toMillis();
      enterPostProcessing(delay);
    } else {
      // this could happen if the service stopped for a long and restarts
      LOG.info("Root CA certificate ID {} in stateful storage has already " +
          "come out of post-processing state", cert.getSerialNumber());
      deleteConfiguration();
    }
  }
}

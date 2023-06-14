/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.security;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Root CA Rotation Manager is a service in SCM to control the CA rotation.
 */
public class RootCARotationManager implements SCMService {

  public static final Logger LOG =
      LoggerFactory.getLogger(RootCARotationManager.class);

  private StorageContainerManager scm;
  private final SCMContext scmContext;
  private OzoneConfiguration ozoneConf;
  private SecurityConfig secConf;
  private ScheduledExecutorService executorService;
  private Duration checkInterval;
  private Duration renewalGracePeriod;
  private Date timeOfDay;
  private CertificateClient scmCertClient;
  private AtomicBoolean isRunning = new AtomicBoolean(false);
  private AtomicBoolean isScheduled = new AtomicBoolean(false);
  private String threadName = this.getClass().getSimpleName();

  /**
   * Constructs RootCARotationManager with the specified arguments.
   *
   * @param scm the storage container manager
   */
  public RootCARotationManager(StorageContainerManager scm) {
    this.scm = scm;
    this.ozoneConf = scm.getConfiguration();
    this.secConf = new SecurityConfig(ozoneConf);
    this.scmContext = scm.getScmContext();

    checkInterval = secConf.getCaCheckInterval();
    timeOfDay = Date.from(LocalDateTime.parse(secConf.getCaRotationTimeOfDay())
        .atZone(ZoneId.systemDefault()).toInstant());
    renewalGracePeriod = secConf.getRenewalGracePeriod();

    executorService = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat(threadName)
            .setDaemon(true).build());

    scmCertClient = scm.getScmCertificateClient();
    scm.getSCMServiceManager().register(this);
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
      }
      return;
    }

    if (isRunning.compareAndSet(false, true)) {
      LOG.info("notifyStatusChanged: enable monitor task");
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
        new MonitorTask(scmCertClient), 0, checkInterval.toMillis(),
        TimeUnit.MILLISECONDS);
    LOG.info("Monitor task for root certificate {} is started with " +
        "interval {}.", scmCertClient.getCACertificate().getSerialNumber(),
        checkInterval);
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  /**
   *  Task to monitor certificate lifetime and start rotation if needed.
   */
  public class MonitorTask implements Runnable {
    private CertificateClient certClient;

    public MonitorTask(CertificateClient client) {
      this.certClient = client;
    }

    @Override
    public void run() {
      Thread.currentThread().setName(threadName +
          (isRunning() ? "-Active" : "-Inactive"));
      if (!isRunning.get() || isScheduled.get()) {
        return;
      }
      // Lock to protect the root CA certificate rotation process,
      // to make sure there is only one task is ongoing at one time.
      synchronized (RootCARotationManager.class) {
        X509Certificate rootCACert = certClient.getCACertificate();
        Duration timeLeft = timeBefore2ExpiryGracePeriod(rootCACert);
        if (timeLeft.isZero()) {
          LOG.info("Root certificate {} has entered the 2 * expiry" +
                  " grace period({}).", rootCACert.getSerialNumber().toString(),
              renewalGracePeriod);
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
                " certificate {} end time {}. Start the rotation immediately.",
                timeToSchedule, rootCACert.getSerialNumber().toString(),
                rootCACert.getNotAfter());
            delay = 0;
          }

          executorService.schedule(new RotationTask(certClient), delay,
              TimeUnit.MILLISECONDS);
          isScheduled.set(true);
          LOG.info("Root certificate {} rotation task is scheduled with {} ms "
              + "delay", rootCACert.getSerialNumber().toString(), delay);
        }
      }
    }
  }

  /**
   *  Task to rotate root certificate.
   */
  public class RotationTask implements Runnable {
    private CertificateClient certClient;

    public RotationTask(CertificateClient client) {
      this.certClient = client;
    }

    @Override
    public void run() {
      if (!isRunning.get()) {
        isScheduled.set(false);
        return;
      }
      // Lock to protect the root CA certificate rotation process,
      // to make sure there is only one task is ongoing at one time.
      // Root CA rotation steps:
      //  1. generate new root CA keys and certificate, persist to disk
      //  2. start new Root CA server
      //  3. send scm Sub-CA rotation preparation request through RATIS
      //  4. send scm Sub-CA rotation commit request through RATIS
      //  5. send scm Sub-CA rotation finish request through RATIS
      synchronized (RootCARotationManager.class) {
        X509Certificate rootCACert = certClient.getCACertificate();
        Duration timeLeft = timeBefore2ExpiryGracePeriod(rootCACert);
        if (timeLeft.isZero()) {
          LOG.info("Root certificate {} rotation is started.",
              rootCACert.getSerialNumber().toString());
          // TODO: start the root CA rotation process
        } else {
          LOG.warn("Root certificate {} hasn't entered the 2 * expiry" +
                  " grace period {}. Skip root certificate rotation this time.",
              rootCACert.getSerialNumber().toString(), renewalGracePeriod);
        }
      }
      isScheduled.set(false);
    }
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
   * Stops scheduled monitor task.
   */
  @Override
  public void stop() {
    try {
      executorService.shutdown();
      if (!executorService.awaitTermination(3, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      // Ignore, we don't really care about the failure.
      Thread.currentThread().interrupt();
    }
  }
}

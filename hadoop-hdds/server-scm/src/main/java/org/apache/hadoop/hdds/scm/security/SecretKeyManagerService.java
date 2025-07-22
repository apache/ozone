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

import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_CERT_STORAGE_DIR;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.LocalSecretKeyStore;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyManager;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyState;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A background service running in SCM to maintain the SecretKeys lifecycle.
 */
public class SecretKeyManagerService implements SCMService, Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecretKeyManagerService.class);

  private static final String SERVICE_NAME =
      SecretKeyManagerService.class.getSimpleName();

  private final SCMContext scmContext;
  private final SecretKeyManager secretKeyManager;
  private final SecretKeyConfig secretKeyConfig;

  /**
   * SCMService related variables.
   */
  private final Lock serviceLock = new ReentrantLock();
  private ServiceStatus serviceStatus = ServiceStatus.PAUSING;

  private final ScheduledExecutorService scheduler;

  public SecretKeyManagerService(SCMContext scmContext,
                                 ConfigurationSource conf,
                                 SCMRatisServer ratisServer) {
    this.scmContext = scmContext;

    secretKeyConfig = new SecretKeyConfig(conf,
        SCM_CA_CERT_STORAGE_DIR);
    SecretKeyStore secretKeyStore = new LocalSecretKeyStore(
        secretKeyConfig.getLocalSecretKeyFile());
    SecretKeyState secretKeyState = new ScmSecretKeyStateBuilder()
        .setSecretKeyStore(secretKeyStore)
        .setRatisServer(ratisServer)
        .build();
    secretKeyManager = new SecretKeyManager(secretKeyState,
        secretKeyStore, secretKeyConfig);

    scheduler = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat(scmContext.threadNamePrefix() + getServiceName())
            .build());

    start();
  }

  @Override
  public void notifyStatusChanged() {
    serviceLock.lock();
    try {
      if (scmContext.isLeaderReady()) {
        // Asynchronously initialize SecretKeys for first time leader.
        if (!secretKeyManager.isInitialized()) {
          scheduler.schedule(() -> {
            try {
              secretKeyManager.checkAndInitialize();
            } catch (Exception e) {
              throw new RuntimeException(
                  "Error replicating initialized state.", e);
            }
          }, 0, TimeUnit.SECONDS);
        }

        serviceStatus = ServiceStatus.RUNNING;
      } else {
        serviceStatus = ServiceStatus.PAUSING;
      }
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public boolean shouldRun() {
    serviceLock.lock();
    try {
      return serviceStatus == ServiceStatus.RUNNING;
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public void run() {
    if (!shouldRun()) {
      return;
    }

    try {
      secretKeyManager.checkAndRotate(false);
    } catch (Exception e) {
      LOG.error("Error occurred when updating SecretKeys.", e);
    }
  }

  @Override
  public String getServiceName() {
    return SERVICE_NAME;
  }

  @Override
  public void start() {
    LOG.info("Scheduling rotation checker with interval {}",
        secretKeyConfig.getRotationCheckDuration());
    scheduler.scheduleAtFixedRate(this, 0,
        secretKeyConfig.getRotationCheckDuration().toMillis(),
        TimeUnit.MILLISECONDS);
  }

  public SecretKeyManager getSecretKeyManager() {
    return secretKeyManager;
  }

  @Override
  public void stop() {
    scheduler.shutdownNow();
  }

  public static boolean isSecretKeyEnable(SecurityConfig conf) {
    return conf.isSecurityEnabled();
  }
}

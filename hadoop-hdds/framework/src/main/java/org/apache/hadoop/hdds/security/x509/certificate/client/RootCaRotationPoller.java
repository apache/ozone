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
package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Poller mechanism for Root Ca Rotation for clients.
 */
public class RootCaRotationPoller implements Runnable, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(RootCaRotationPoller.class);
  private final List<Consumer<List<X509Certificate>>> rootCaListConsumers;
  private final ScheduledExecutorService poller;
  private final Duration pollingRate;
  private Set<X509Certificate> knownRootCerts;
  private final SCMSecurityProtocolClientSideTranslatorPB scmSecureClient;

  public RootCaRotationPoller(SecurityConfig securityConfig,
      Set<X509Certificate> initiallyKnownRootCaCerts,
      SCMSecurityProtocolClientSideTranslatorPB scmSecureClient) {
    this.scmSecureClient = scmSecureClient;
    this.knownRootCerts = initiallyKnownRootCaCerts;
    poller = Executors.newSingleThreadScheduledExecutor();
    pollingRate = securityConfig.getRootCaClientPollingFrequency();
    rootCaListConsumers = new ArrayList<>();
  }

  private void pollRootCas() {
    try {
      List<String> pemEncodedRootCaList =
          scmSecureClient.getAllRootCaCertificates();
      List<X509Certificate> scmRootCaCerts =
          OzoneSecurityUtil.convertToX509(pemEncodedRootCaList);
      if (!knownRootCerts.containsAll(scmRootCaCerts)) {
        rootCaListConsumers.forEach(c -> c.accept(scmRootCaCerts));
        knownRootCerts = new HashSet<>();
        knownRootCerts.addAll(scmRootCaCerts);
      }
    } catch (IOException e) {
      LOG.error("Error while trying to rotate root ca certificate", e);
    }
  }

  public void addRootCaRotationConsumer(
      Consumer<List<X509Certificate>> consumer) {
    rootCaListConsumers.add(consumer);
  }

  @Override
  public void run() {
    poller.scheduleAtFixedRate(this::pollRootCas, 0,
        pollingRate.getSeconds(), TimeUnit.SECONDS);
  }

  @Override
  public void close() {
    executorServiceShutdownGraceful(poller);
  }

  private void executorServiceShutdownGraceful(ExecutorService executor) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.error("Unable to shutdown state machine properly.");
      }
    } catch (InterruptedException e) {
      LOG.error("Error attempting to shutdown.", e);
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}

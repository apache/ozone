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

package org.apache.hadoop.hdds.security.x509.certificate.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Poller mechanism for Root Ca Rotation for clients.
 */
public class RootCaRotationPoller implements Runnable, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(RootCaRotationPoller.class);
  private final List<Function<List<X509Certificate>, CompletableFuture<Void>>>
      rootCARotationProcessors;
  private final ScheduledExecutorService poller;
  private final Duration pollingInterval;
  private Set<X509Certificate> knownRootCerts;
  private final SCMSecurityProtocolClientSideTranslatorPB scmSecureClient;
  private final AtomicBoolean certificateRenewalError;

  public RootCaRotationPoller(SecurityConfig securityConfig,
      Set<X509Certificate> initiallyKnownRootCaCerts,
      SCMSecurityProtocolClientSideTranslatorPB scmSecureClient,
      String threadNamePrefix) {
    this.scmSecureClient = scmSecureClient;
    this.knownRootCerts = initiallyKnownRootCaCerts;
    poller = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder()
            .setNameFormat(threadNamePrefix + getClass().getSimpleName())
            .setDaemon(true).build());
    pollingInterval = securityConfig.getRootCaCertificatePollingInterval();
    rootCARotationProcessors = new ArrayList<>();
    certificateRenewalError = new AtomicBoolean(false);
  }

  /**
   * Polls the SCM for root ca certificates and compares them to the known
   * set of root ca certificates. If there are new root ca certificates it
   * invokes the necessary handlers provided in rootCaRotationProcessors.
   *
   * @return returns true if the SCM provided a new root ca certificate.
   */
  void pollRootCas() {
    try {
      List<String> pemEncodedRootCaList =
          scmSecureClient.getAllRootCaCertificates();
      List<X509Certificate> rootCAsFromSCM =
          OzoneSecurityUtil.convertToX509(pemEncodedRootCaList);
      List<X509Certificate> scmCertsWithoutKnownCerts
          = new ArrayList<>(rootCAsFromSCM);
      scmCertsWithoutKnownCerts.removeAll(knownRootCerts);
      if (scmCertsWithoutKnownCerts.isEmpty()) {
        return;
      }
      LOG.info("Some root CAs are not known to the client out of the root " +
          "CAs known to the SCMs. Root CA Cert ids known to the client: " +
          getPrintableCertIds(knownRootCerts) + ". Root CA Cert ids from " +
          "SCM not known by the client: " +
          getPrintableCertIds(scmCertsWithoutKnownCerts));
      certificateRenewalError.set(false);
      CompletableFuture<Void> allRootCAProcessorFutures =
          CompletableFuture.allOf(rootCARotationProcessors.stream()
              .map(c -> c.apply(rootCAsFromSCM))
              .toArray(CompletableFuture[]::new));

      allRootCAProcessorFutures.whenComplete((unused, throwable) -> {
        if (throwable == null && !certificateRenewalError.get()) {
          knownRootCerts = new HashSet<>(rootCAsFromSCM);
          LOG.info("Certificate processing was successful.");
        } else {
          LOG.info("Certificate consumption was unsuccessful. " +
              (certificateRenewalError.get() ?
                  "There was a caught exception when trying to sign the " +
                      "certificate" :
                  "There was an unexpected error during cert rotation" +
                      throwable));
        }
      });
    } catch (IOException e) {
      LOG.error("Error while trying to poll root ca certificate", e);
    }
  }

  public void addRootCARotationProcessor(
      Function<List<X509Certificate>, CompletableFuture<Void>> processor) {
    rootCARotationProcessors.add(processor);
  }

  @Override
  public void run() {
    poller.scheduleAtFixedRate(this::pollRootCas, 0,
        pollingInterval.getSeconds(), TimeUnit.SECONDS);
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
        LOG.warn("{} couldn't be shut down gracefully",
            getClass().getSimpleName());
      }
    } catch (InterruptedException e) {
      LOG.warn("{} couldn't be stopped gracefully", getClass().getSimpleName());
      Thread.currentThread().interrupt();
    }
  }

  public void setCertificateRenewalError() {
    certificateRenewalError.set(true);
  }

  private String getPrintableCertIds(Collection<X509Certificate> certs) {
    return certs.stream()
        .map(X509Certificate::getSerialNumber)
        .map(BigInteger::toString)
        .collect(Collectors.joining(", "));
  }
}

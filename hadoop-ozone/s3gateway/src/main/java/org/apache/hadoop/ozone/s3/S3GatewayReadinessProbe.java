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

package org.apache.hadoop.ozone.s3;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.conf.S3GatewayHealthCheckConfig;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backs the S3 Gateway readiness endpoint (/health/ready).
 *
 * <p>A single background thread periodically probes OM reachability and stores
 * the result in a volatile flag. The servlet only reads that flag, so
 * /health/ready responds immediately (no OM RPC on the request path) and a
 * load balancer polling it can never be blocked by a slow or unreachable OM.
 *
 * <p>Readiness starts as {@code false} and flips to {@code true} only after a
 * probe succeeds, so the endpoint reports "not ready" during startup (before
 * the first successful probe) without waiting for the first S3 request. The
 * probe owns a dedicated {@link OzoneClient} created through
 * {@link OzoneClientCache#createClient}, so it exercises the same OM transport
 * the gateway serves with, independent of the lazily-created CDI client on the
 * S3 listener.
 */
public class S3GatewayReadinessProbe implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3GatewayReadinessProbe.class);

  private final OzoneConfiguration conf;
  private final long intervalMillis;
  private final long timeoutMillis;

  private final ScheduledExecutorService scheduler;
  private final ExecutorService probeExecutor;

  private volatile boolean ready = false;
  private volatile OzoneClient client;

  S3GatewayReadinessProbe(OzoneConfiguration conf,
      S3GatewayHealthCheckConfig healthConfig) {
    this.conf = conf;
    this.intervalMillis = healthConfig.getProbeInterval();
    this.timeoutMillis = healthConfig.getProbeTimeout();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat("s3g-readiness-probe-%d")
            .setDaemon(true)
            .build());
    this.probeExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat("s3g-readiness-om-%d")
            .setDaemon(true)
            .build());
  }

  public void start() {
    scheduler.scheduleWithFixedDelay(this::probe, 0, intervalMillis,
        TimeUnit.MILLISECONDS);
  }

  public boolean isReady() {
    return ready;
  }

  /**
   * Runs one probe on the scheduler thread, bounded by the configured timeout.
   * The OM call runs on a separate single-threaded executor so a hung call
   * cannot block the scheduler and gets an enforced deadline: on timeout the
   * gateway is reported not ready rather than staying stuck at ready.
   */
  private void probe() {
    Future<Void> future = probeExecutor.submit(() -> {
      OzoneClient c = client;
      if (c == null) {
        c = OzoneClientCache.createClient(probeClientConf());
        client = c;
      }
      c.getObjectStore().getClientProxy().getOzoneManagerClient()
          .getServiceInfo();
      return null;
    });
    try {
      future.get(timeoutMillis, TimeUnit.MILLISECONDS);
      ready = true;
    } catch (TimeoutException e) {
      ready = false;
      future.cancel(true);
      LOG.debug("Readiness probe timed out after {} ms", timeoutMillis);
    } catch (Exception e) {
      ready = false;
      LOG.debug("Readiness probe failed; reporting not ready", e);
    }
  }

  /**
   * A copy of the gateway config tuned for probing: S3 auth is disabled
   * (getServiceInfo does not need S3 credentials) and the OM RPC timeout is
   * bounded so a stuck probe thread eventually unblocks.
   */
  private OzoneConfiguration probeClientConf() {
    OzoneConfiguration probeConf = new OzoneConfiguration(conf);
    probeConf.setBoolean(S3Auth.S3_AUTH_CHECK, false);
    probeConf.setBoolean("ipc.client.ping", false);
    probeConf.setTimeDuration("ozone.om.client.rpc.timeout", timeoutMillis,
        TimeUnit.MILLISECONDS);
    return probeConf;
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
    probeExecutor.shutdownNow();
    IOUtils.close(LOG, client);
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.ssl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.function.Consumer;

/**
 * Implements basic logic to track when security materials changes and call
 * the action passed to the constructor when it does. An exception handler
 * can optionally also be specified in the constructor, otherwise any
 * exception occurring during process will be logged using this class' logger.
 */
@InterfaceAudience.Private
public class MonitoringTimerTask extends TimerTask {

  static final Logger LOG = LoggerFactory.getLogger(MonitoringTimerTask.class);

  @VisibleForTesting
  static final String PROCESS_ERROR_MESSAGE =
      "Could not process security materials change : ";

  private final CertificateClient caClient;
  private final Consumer<CertificateClient> onReload;
  private final Consumer<Throwable> onReloadFailure;

  /**
   * Create monitoring task to be scheduled by a java.util.Timer instance.
   * @param caClient client that holds all security materials.
   * @param onReload What to do when the security materials change.
   * @param onReloadFailure What to do when <code>onReload</code>
   *                        throws an exception.
   */
  public MonitoringTimerTask(CertificateClient caClient,
      Consumer<CertificateClient> onReload,
      Consumer<Throwable> onReloadFailure) {
    Preconditions.checkNotNull(caClient, "caClient is not set");
    Preconditions.checkNotNull(onReload, "action of reload is not set");

    this.caClient = caClient;
    this.onReload = onReload;
    this.onReloadFailure = onReloadFailure;
  }

  @Override
  public void run() {
    if (caClient.isCertificateRenewed()) {
      try {
        onReload.accept(caClient);
      } catch (Throwable t) {
        if (onReloadFailure != null) {
          onReloadFailure.accept(t);
        } else {
          LOG.error(PROCESS_ERROR_MESSAGE, t);
        }
      }
    }
  }
}

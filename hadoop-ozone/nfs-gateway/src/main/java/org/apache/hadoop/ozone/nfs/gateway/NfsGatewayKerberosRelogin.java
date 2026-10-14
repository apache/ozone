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

package org.apache.hadoop.ozone.nfs.gateway;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Periodically renews the NFS gateway's keytab credentials while it is serving requests. */
final class NfsGatewayKerberosRelogin implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(NfsGatewayKerberosRelogin.class);
  private static final long CHECK_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("NfsGatewayKerberosRelogin-%d").setDaemon(true).build());

  NfsGatewayKerberosRelogin(UserGroupInformation loginUser) {
    this(loginUser, CHECK_INTERVAL_MILLIS);
  }

  NfsGatewayKerberosRelogin(UserGroupInformation loginUser, long intervalMillis) {
    this(() -> check(loginUser), intervalMillis);
  }

  NfsGatewayKerberosRelogin(Runnable relogin, long intervalMillis) {
    executor.scheduleAtFixedRate(relogin, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
  }

  private static void check(UserGroupInformation loginUser) {
    try {
      loginUser.checkTGTAndReloginFromKeytab();
    } catch (IOException ex) {
      LOG.warn("Unable to renew NFS gateway Kerberos credentials from keytab", ex);
    }
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}

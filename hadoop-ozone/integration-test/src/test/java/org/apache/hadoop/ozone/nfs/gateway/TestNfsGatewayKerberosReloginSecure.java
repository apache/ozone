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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.security.AccessController;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Verify scheduled gateway login renews a TGT at Hadoop's normal renewal threshold. */
@Tag("nfs")
class TestNfsGatewayKerberosReloginSecure {

  private static final String PRINCIPAL = "nfs/localhost";
  private static final long TICKET_LIFETIME_SECONDS = 10;
  private static final long RENEWAL_CHECK_INTERVAL_MILLIS = 50;

  private static MiniKdc miniKdc;
  private static File workDir;
  private static File keytab;

  @BeforeAll
  static void startKdc() throws Exception {
    workDir = Files.createTempDirectory("nfs-kerberos-renewal").toFile();
    Properties kdcConfig = MiniKdc.createConf();
    kdcConfig.setProperty(MiniKdc.MAX_TICKET_LIFETIME, Long.toString(TICKET_LIFETIME_SECONDS));
    miniKdc = new MiniKdc(kdcConfig, workDir);
    miniKdc.start();
    keytab = new File(workDir, "nfs.keytab");
    miniKdc.createPrincipal(keytab, PRINCIPAL);

    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    conf.setLong("hadoop.kerberos.min.seconds.before.relogin", 0);
    UserGroupInformation.setConfiguration(conf);
  }

  @AfterAll
  static void stopKdc() {
    if (miniKdc != null) {
      miniKdc.stop();
    }
    if (workDir != null) {
      org.apache.commons.io.FileUtils.deleteQuietly(workDir);
    }
  }

  @Test
  void scheduledGatewayReloginRenewsTgtNearNaturalExpiry() throws Exception {
    UserGroupInformation gatewayUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        PRINCIPAL + "@" + miniKdc.getRealm(), keytab.getAbsolutePath());
    KerberosTicket originalTgt = findTgt(gatewayUser);
    assertNotNull(originalTgt, "Keytab login should acquire a TGT");
    Date originalStartTime = originalTgt.getStartTime();
    Date originalEndTime = originalTgt.getEndTime();
    AtomicReference<Date> renewalTime = new AtomicReference<>();
    assertTrue(originalTgt.getEndTime().getTime() - System.currentTimeMillis()
        <= TimeUnit.SECONDS.toMillis(TICKET_LIFETIME_SECONDS),
        "MiniKdc should issue a short-lived TGT, got end time " + originalTgt.getEndTime());

    try (NfsGatewayKerberosRelogin ignored = new NfsGatewayKerberosRelogin(
        gatewayUser, RENEWAL_CHECK_INTERVAL_MILLIS)) {
      GenericTestUtils.waitFor(() -> {
        try {
          KerberosTicket currentTgt = findTgt(gatewayUser);
          boolean renewed = currentTgt != null && currentTgt != originalTgt
              && currentTgt.getEndTime() != null && currentTgt.getEndTime().after(originalEndTime);
          if (renewed) {
            renewalTime.set(new Date());
          }
          return renewed;
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }, (int) RENEWAL_CHECK_INTERVAL_MILLIS, 10_000);
    }

    KerberosTicket renewedTgt = findTgt(gatewayUser);
    assertNotNull(renewedTgt);
    long renewalWindowStart = originalStartTime.getTime()
        + (long) ((originalEndTime.getTime() - originalStartTime.getTime()) * 0.75);
    assertTrue(renewalTime.get().getTime() >= renewalWindowStart,
        "TGT should not renew well before Hadoop's normal 80% refresh threshold");
    assertTrue(renewalTime.get().before(originalEndTime), "TGT should renew before the original expires");
    assertTrue(renewedTgt.getEndTime().after(originalEndTime),
        "Scheduled renewal should replace the TGT before the original expires");
  }

  private static KerberosTicket findTgt(UserGroupInformation user) throws Exception {
    return user.doAs((java.security.PrivilegedExceptionAction<KerberosTicket>) () -> {
      for (KerberosTicket ticket : Subject.getSubject(AccessController.getContext())
            .getPrivateCredentials(KerberosTicket.class)) {
        if (!ticket.isDestroyed() && ticket.getEndTime() != null
            && ticket.getServer().getName().startsWith("krbtgt/")) {
          return ticket;
        }
      }
      return null;
    });
  }
}

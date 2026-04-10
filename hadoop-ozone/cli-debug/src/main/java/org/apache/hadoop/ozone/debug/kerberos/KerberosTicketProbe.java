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

package org.apache.hadoop.ozone.debug.kerberos;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Checks the current Kerberos authentication state of the process.
 * This probe verifies whether Kerberos security is enabled in configuration,
 * what authentication method Hadoop is currently using, and whether a valid
 * Kerberos ticket is present in the process ticket cache.
 */
public class KerberosTicketProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Kerberos Ticket";
  }

  @Override
  public ProbeResult test(OzoneConfiguration conf) {

    try {
      UserGroupInformation.setConfiguration(conf);
      String authType = conf.getTrimmed(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "simple");
      boolean kerberosEnabled =
          "kerberos".equalsIgnoreCase(authType);
      printValue("kerberos configured", String.valueOf(kerberosEnabled));

      // If Kerberos is not enabled, do not proceed with UGI calls.
      if (!kerberosEnabled) {
        warn("Kerberos is not enabled in configurations (current=" + authType + ")");
        return ProbeResult.WARN;
      }

      UserGroupInformation ugi =
          UserGroupInformation.getLoginUser();
      printValue("Login user", ugi.getUserName());

      printValue("Authentication method",
          String.valueOf(ugi.getAuthenticationMethod()));

      boolean usingKerberos =
          ugi.getAuthenticationMethod()
              == UserGroupInformation.AuthenticationMethod.KERBEROS;

      boolean hasTicket = ugi.hasKerberosCredentials();
      printValue("Kerberos ticket present", String.valueOf(hasTicket));

      String ticketCache = System.getenv("KRB5CCNAME");

      printValue("Ticket cache",
          ticketCache == null ? "(default cache)" : ticketCache);

      if (!usingKerberos) {
        warn("Kerberos is configured but not active.\n"
            + "  Authentication method = " + ugi.getAuthenticationMethod() + "\n"
            + "  No Kerberos login detected.\n"
            + "  Fix: Run 'kinit' or login via keytab.");
        return ProbeResult.WARN;
      }

      if (!hasTicket) {
        warn("Kerberos authentication is enabled but no valid ticket is found.\n"
            + "  Fix: Run 'kinit' to obtain Kerberos credentials.");
        return ProbeResult.WARN;
      }
      printValue("Kerberos status",
          "Authentication is active and ticket is valid");
      return ProbeResult.PASS;
    } catch (Exception e) {
      error("Kerberos check failure: " + e.getMessage());
      return ProbeResult.FAIL;
    }
  }
}

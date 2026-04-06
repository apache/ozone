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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Checks the current Kerberos authentication state of the process.
 *
 * This probe verifies whether Kerberos security is enabled in configuration,
 * what authentication method Hadoop is currently using, and whether a valid
 * Kerberos ticket is present in the process ticket cache.
 *
 * The probe does NOT attempt to perform a login (kinit), Instead it reports
 * the current state so operators can diagnose security issues.
 * Checks the current Kerberos authentication state of the process.
 * Provides clear diagnostics about:
 * - Whether Kerberos is configured.
 * - Whether the process is using Kerberos.
 * - Whether a valid ticket is present.
 */
public class KerberosTicketProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Kerberos Ticket";
  }

  @Override
  public boolean test(OzoneConfiguration conf) {

    try {
      UserGroupInformation.setConfiguration(conf);
      String authType = conf.getTrimmed("hadoop.security.authentication");
      boolean kerberosEnabled =
          "kerberos".equalsIgnoreCase(authType);
      System.out.println("kerberos configured = " + kerberosEnabled);

      UserGroupInformation ugi =
          UserGroupInformation.getLoginUser();
      System.out.println("Login user = " + ugi.getUserName());

      boolean usingKerberos =
          ugi.getAuthenticationMethod()
              == UserGroupInformation.AuthenticationMethod.KERBEROS;

      System.out.println("Authentication method = "
          + ugi.getAuthenticationMethod());

      boolean hasTicket = ugi.hasKerberosCredentials();
      System.out.println("Kerberos ticket present = " + hasTicket);

      String ticketCache = System.getenv("KRB5CCNAME");

      System.out.println("Ticket cache = "
          + (ticketCache == null ? "(default cache)" : ticketCache));

      if (!kerberosEnabled) {
        warn("Kerberos is not enabled in configurations (current=" + authType + ")");
        return true; //not a failure, just informational
      }

      if (!usingKerberos) {
        warn("Kerberos is configured but not active.\n"
            + "  Authentication method = " + ugi.getAuthenticationMethod() + "\n"
            + "  No Kerberos login detected.\n"
            + "  Fix: Run 'kinit' or login via keytab.");
        return false;
      }

      if (!hasTicket) {
        warn("Kerberos authentication is enabled but no valid ticket is found.\n"
            + "  Fix: Run 'kinit' to obtain Kerberos credentials.");
        return false;
      }
      System.out.println("Kerberos authentication is active and ticket is valid");
      return true;
    } catch (Exception e) {
      error("Kerberos check failure: " + e.getMessage());
      return false;
    }
  }
}

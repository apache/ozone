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

package org.apache.hadoop.ozone.debug.kdiag;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Checks the current Kerberos authentication state of the process.
 *
 * This probe verifies whether Kerberos security is enabled in configuration,
 * what authentication method Hadoop is currently using, and whether a valid
 * Kerberos ticket is present in the process ticket cache.
 *
 * The probe does NOT attempt to perform a login (kinit). Instead it reports
 * the current state so operators can diagnose security issues.
 */
public class KerberosTicketProbe implements DiagnosticProbe {

  @Override
  public String name() {
    return "Kerberos Ticket";
  }

  @Override
  public boolean run() {
    System.out.println("-- Kerberos Ticket --");
    try {
      OzoneConfiguration conf = new OzoneConfiguration();
      // Initialize Hadoop security configuration
      UserGroupInformation.setConfiguration(conf);
      String authType = conf.get("hadoop.security.authentication");
      boolean securityEnabled =
          "kerberos".equalsIgnoreCase(authType);
      System.out.println("Security enabled = " + securityEnabled);
      UserGroupInformation ugi =
          UserGroupInformation.getCurrentUser();
      System.out.println("Login user = " + ugi.getUserName());
      System.out.println("Authentication method = "
          + ugi.getAuthenticationMethod());
      boolean hasTicket = ugi.hasKerberosCredentials();
      System.out.println("Kerberos ticket present = " + hasTicket);
      String ticketCache = System.getenv("KRB5CCNAME");
      System.out.println("Ticket cache = "
          + (ticketCache == null ? "(default cache)" : ticketCache));

      if (!securityEnabled) {
        System.out.println(
            "Kerberos security is not enabled in configuration");
        return true;
      }

      if (!hasTicket) {
        System.out.println(
            "WARNING: Kerberos security is enabled but no ticket is loaded");
        System.out.println(
            "Run 'kinit' to obtain Kerberos credentials");
        return false;
      }
      return true;
    } catch (Exception e) {
      System.out.println(
          "ERROR checking kerberos credentials: " + e.getMessage());
      return false;
    }
  }
}

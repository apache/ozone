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

import java.io.File;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Validates JVM-level Kerberos related system properties.
 *  Verifies that the configured krb5.conf file exists.
 *  Detects partial configuration (realm without KDC or vice versa).
 *  Warns about conflicting configurations (both krb5.conf and explicit realm/KDC).
 *  Indicates if Kerberos debug logging is enabled.
 */
public class JvmKerberosProbe extends ConfigProbe {

  @Override
  public String name() {
    return "JVM Kerberos Properties";
  }

  @Override
  public ProbeResult test(OzoneConfiguration conf) {

    ProbeResult result = ProbeResult.PASS;
    String krb5Conf = System.getProperty("java.security.krb5.conf");
    String realm = System.getProperty("java.security.krb5.realm");
    String kdc = System.getProperty("java.security.krb5.kdc");
    String debug = System.getProperty("sun.security.krb5.debug");

    // Print JVM Kerberos related system properties.
    printValue("java.security.krb5.conf", krb5Conf);
    printValue("java.security.krb5.realm", realm);
    printValue("java.security.krb5.kdc", kdc);
    printValue("sun.security.krb5.debug", debug);

    // If krb5.conf is not set at JVM level,
    // fallback to default system path.
    if (krb5Conf == null || krb5Conf.isEmpty()) {
      krb5Conf = "/etc/krb5.conf";
      printValue("Effective krb5.conf (default)", krb5Conf);
    }

    File file = new File(krb5Conf);
    if (!canReadFile(file, "krb5.conf")) {
      result = ProbeResult.FAIL;
    }

    // Partial config
    if (realm != null && kdc == null) {
      warn("java.security.krb5.realm is set but " +
          "java.security.krb5.kdc is not set");
      if (result == ProbeResult.PASS) {
        result = ProbeResult.WARN;
      }
    }

    if (kdc != null && realm == null) {
      warn("java.security.krb5.kdc is set but " +
          "java.security.krb5.realm is not set");
      if (result == ProbeResult.PASS) {
        result = ProbeResult.WARN;
      }
    }

    // Conflicting config
    if (realm != null || kdc != null) {
      // Conflict only matters if both styles are used together
      if (System.getProperty("java.security.krb5.conf") != null) {
        warn("Both krb5.conf and explicit realm/kdc are set, may cause conflicts");
        if (result == ProbeResult.PASS) {
          result = ProbeResult.WARN;
        }
      }
    }

    // Debug info
    if ("true".equalsIgnoreCase(debug)) {
      printValue("Kerberos debug", "ENABLED");
    }
    return result;
  }
}

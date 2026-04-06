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
 * Prints JVM Kerberos related system properties.
 */
public class JvmKerberosProbe extends ConfigProbe {

  @Override
  public String name() {
    return "JVM Kerberos Properties";
  }

  @Override
  public boolean test(OzoneConfiguration conf) {

    boolean valid = true;
    String krb5Conf = System.getProperty("java.security.krb5.conf");
    String realm = System.getProperty("java.security.krb5.realm");
    String kdc = System.getProperty("java.security.krb5.kdc");
    String debug = System.getProperty("sun.security.krb5.debug");
    // Print JVM Kerberos related system properties.
    printValue("java.security.krb5.conf", krb5Conf);
    printValue("java.security.krb5.realm", realm);
    printValue("java.security.krb5.kdc", kdc);
    printValue("sun.security.krb5.debug", debug);

    // Validate krb5.conf path
    if (krb5Conf != null) {
      File file = new File(krb5Conf);
      if (!file.exists()) {
        error("Configured krb5.conf does not exist: " + krb5Conf);
        valid = false;
      }
    }

    // Partial config
    if (realm != null && kdc == null) {
      warn("java.security.krb5.realm is set but " +
          "java.security.krb5.kdc is not set");
    }

    if (kdc != null && realm == null) {
      warn("java.security.krb5.kdc is set but " +
          "java.security.krb5.realm is not set");
    }

    // Conflicting config
    if (krb5Conf != null && (realm != null || kdc != null)) {
      warn("Both krb5.conf and explicit realm/kdc are set; may cause conflicts");
    }

    // Debug info
    if ("true".equalsIgnoreCase(debug)) {
      System.out.println("Kerberos debug logging is ENABLED");
    }
    return valid;
  }
}

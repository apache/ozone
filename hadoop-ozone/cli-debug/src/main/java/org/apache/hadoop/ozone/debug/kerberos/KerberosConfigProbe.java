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
import org.apache.hadoop.security.authentication.util.KerberosUtil;

/**
 * Validates system-level Kerberos configuration (krb5.conf) and realm.
 *  Location of krb5.conf (from KRB5_CONFIG or default /etc/krb5.conf)
 *  file existence and readability.
 *  Default Kerberos realm resolution using KerberosUtil.
 */
public class KerberosConfigProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Kerberos Configuration";
  }

  @Override
  public ProbeResult test(OzoneConfiguration conf) {
    // Determine krb5.conf location
    String path = System.getenv("KRB5_CONFIG");
    if (path == null || path.isEmpty()) {
      path = "/etc/krb5.conf";
    }
    File file = new File(path);
    printValue("krb5.conf", file.toString());

    // Avoid scenario where file does exist but not accessible.
    // Validate actual readability (not just permissions)
    if (!canReadFile(file, "krb5.conf")) {
      return ProbeResult.FAIL;
    }
    try {
      printValue("Default realm",
          KerberosUtil.getDefaultRealm());
      return ProbeResult.PASS;
    } catch (Exception e) {
      error("Cannot determine Kerberos realm: " + e.getMessage());
      return ProbeResult.FAIL;
    }
  }
}

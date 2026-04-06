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
 * Validates Kerberos configuration file and realm.
 *
 * This probe checks:
 * - Location of krb5.conf
 * - Default Kerberos realm
 * - JVM Kerberos system properties used by Java security
 */
public class KerberosConfigProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Kerberos Configuration";
  }

  @Override
  public boolean test(OzoneConfiguration conf) {
    // Determine krb5.conf location
    String path = System.getenv("KRB5_CONFIG");
    if (path == null) {
      path = "/etc/krb5.conf";
    }
    File file = new File(path);
    System.out.println("krb5.conf = " + file);
    if (!file.exists()) {
      error("krb5.conf not found");
      return false;
    }
    // Avoid scenario where file does exist but not accessible.
    // Validate actual readability (not just permissions)
    if (!canReadFile(file, "krb5.conf")) {
      return false;
    }
    try {
      System.out.println("Default realm = "
          + KerberosUtil.getDefaultRealm());
      return true;
    } catch (Exception e) {
      error("Cannot determine Kerberos realm: " + e.getMessage());
      return false;
    }
  }
}

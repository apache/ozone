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

import java.io.File;
import org.apache.hadoop.security.authentication.util.KerberosUtil;

/**
 * Validates Kerberos configuration file and realm.
 *
 * This probe checks:
 * - Location of krb5.conf
 * - Default Kerberos realm
 * - JVM Kerberos system properties used by Java security
 */
public class KerberosConfigProbe implements DiagnosticProbe {

  @Override
  public String name() {
    return "Kerberos Configuration";
  }

  @Override
  public boolean run() {
    System.out.println("-- Kerberos Configuration --");
    // Determine krb5.conf location
    String path = System.getenv("KRB5_CONFIG");
    if (path == null) {
      path = "/etc/krb5.conf";
    }
    File file = new File(path);
    System.out.println("krb5.conf = " + file);
    try {
      String realm = KerberosUtil.getDefaultRealm();
      System.out.println("Default realm = " + realm);
    } catch (Exception e) {
      System.out.println("WARNING: Unable to determine default realm");
      return false;
    }
    return file.exists();
  }
}

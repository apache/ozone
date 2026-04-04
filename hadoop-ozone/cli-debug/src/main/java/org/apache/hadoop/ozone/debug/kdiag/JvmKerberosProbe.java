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

/**
 * Prints JVM Kerberos related system properties.
 */
public class JvmKerberosProbe implements DiagnosticProbe {

  @Override
  public String name() {
    return "JVM Kerberos Properties";
  }

  @Override
  public boolean run() {

    System.out.println("-- JVM Kerberos Properties --");
    // Print JVM Kerberos related system properties
    print("java.security.krb5.conf");
    print("java.security.krb5.realm");
    print("java.security.krb5.kdc");
    print("sun.security.krb5.debug");

    return true;
  }

  private void print(String key) {

    String value = System.getProperty(key);
    System.out.println(key + " = "
        + (value == null ? "(unset)" : value));
  }
}

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

/**
 * Validates Hadoop and Ozone security configuration.
 *
 * This probe verifies that Kerberos authentication is enabled
 * and prints related security configuration values used by
 * Ozone services.
 */
public class SecurityConfigProbe implements DiagnosticProbe {

  @Override
  public String name() {
    return "Security Configuration";
  }

  @Override
  public boolean run() {
    System.out.println("-- Security Configuration --");
    OzoneConfiguration conf = new OzoneConfiguration();
    boolean ok = true;
    ok &= print(conf, "hadoop.security.authentication");
    print(conf, "ozone.security.enabled");
    print(conf, "ozone.security.http.kerberos.enabled");
    print(conf, "hadoop.rpc.protection");
    print(conf, "hadoop.security.saslproperties.resolver.class");
    print(conf, "ozone.administrators");
    print(conf, "ozone.s3.administrators");
    print(conf, "hdds.block.token.enabled");
    print(conf, "hdds.container.token.enabled");
    print(conf, "hdds.grpc.tls.enabled");
    String auth = conf.get("hadoop.security.authentication");
    if (auth == null) {
      System.out.println("WARNING: authentication property not configured");
      return false;
    }
    if (!"kerberos".equalsIgnoreCase(auth)) {
      System.out.println("WARNING: Kerberos security is not enabled");
      return false;
    }
    System.out.println("Kerberos security is enabled");
    return ok;
  }

  private boolean print(OzoneConfiguration conf, String key) {
    String value = conf.get(key);
    System.out.println(key + " = " +
        (value == null ? "(unset)" : value));
    return true;
  }
}

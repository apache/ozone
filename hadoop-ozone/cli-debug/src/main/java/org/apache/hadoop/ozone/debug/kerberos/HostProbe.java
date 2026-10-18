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

import java.net.InetAddress;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * HostProbe to handle system-level failures gracefully and report actionable errors instead of failing silently.
 */
public class HostProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Host Information";
  }

  @Override
  public ProbeResult test(OzoneConfiguration conf) throws Exception {

    ProbeResult result = ProbeResult.PASS;
    // Hostname resolution checks
    try {
      String hostname = InetAddress.getLocalHost().getCanonicalHostName();
      printValue("Hostname", hostname);
    } catch (Exception e) {
      error("Failed to resolve hostname: " + e.getMessage());
      result = ProbeResult.FAIL;
    }

    // User checks
    try {
      String user = System.getProperty("user.name");
      if (user == null || user.isEmpty()) {
        error("User name is not available");
        result = ProbeResult.FAIL;
      } else {
        printValue("User", user);
      }
    } catch (Exception e) {
      error("Failed to determine user: " + e.getMessage());
      result = ProbeResult.FAIL;
    }

    // Java version
    try {
      printValue("Java version",
          System.getProperty("java.version"));
    } catch (Exception e) {
      error("Failed to get Java version: " + e.getMessage());
      if (result == ProbeResult.PASS) {
        result = ProbeResult.WARN;
      }
    }
    return result;
  }
}

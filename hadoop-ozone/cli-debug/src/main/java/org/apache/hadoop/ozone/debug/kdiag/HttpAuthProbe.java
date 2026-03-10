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
 * Prints HTTP Kerberos authentication configuration for Ozone services.
 *
 * This probe checks whether the HTTP endpoints (WebUI / REST services)
 * of Ozone components are configured to use Kerberos authentication.
 *
 * It only prints configuration values for diagnostics and does not
 * enforce validation. Validation can be added in future improvements.
 */
public class HttpAuthProbe implements DiagnosticProbe {

  @Override
  public String name() {
    return "HTTP Kerberos Authentication";
  }

  @Override
  public boolean run() {

    System.out.println("-- HTTP Kerberos Authentication --");

    OzoneConfiguration conf = new OzoneConfiguration();

    print(conf, "ozone.om.http.auth.type");
    print(conf, "hdds.scm.http.auth.type");
    print(conf, "hdds.datanode.http.auth.type");
    print(conf, "ozone.s3g.http.auth.type");
    print(conf, "ozone.recon.http.auth.type");

    return true;
  }

  /**
   * Helper method to print configuration value.
   */
  private void print(OzoneConfiguration conf, String key) {

    String value = conf.get(key);
    System.out.println(key + " = " +
        (value == null ? "(unset)" : value));
  }
}

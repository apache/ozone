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
import org.apache.hadoop.security.authentication.util.KerberosName;

/**
 * Validates auth_to_local principal mapping.
 */
public class PrincipalMappingProbe implements DiagnosticProbe {

  @Override
  public String name() {
    return "auth_to_local mapping";
  }

  @Override
  public boolean run() throws Exception {
    System.out.println("-- Principal Mapping --");
    OzoneConfiguration conf = new OzoneConfiguration();
    String rules = conf.get("hadoop.security.auth_to_local");
    if (rules == null) {
      System.out.println("auth_to_local rules not configured");
      return false;
    }
    KerberosName.setRules(rules);
    String principal =
        UserGroupInformation.getLoginUser().getUserName();
    KerberosName name =
        new KerberosName(principal);
    System.out.println("Principal = " + principal);
    System.out.println("Local user = " + name.getShortName());
    return true;
  }
}

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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;

/**
 * Validates auth_to_local principal mapping.
 */
public class PrincipalMappingProbe extends ConfigProbe {

  @Override
  public String name() {
    return "auth_to_local mapping";
  }

  @Override
  public boolean test(OzoneConfiguration conf) {

    // Read auth_to_local rules
    String rules = conf.getTrimmed("hadoop.security.auth_to_local");

    if (rules == null || rules.isEmpty()) {
      warn("auth_to_local rules not configured");
      return false;
    }
    System.out.println("auth_to_local rules = " + rules);

    try {
      // Apply rules
      KerberosName.setRules(rules);
      // Get current user principal
      String principal =
          UserGroupInformation.getLoginUser().getUserName();
      System.out.println("Principal = " + principal);
      KerberosName name = new KerberosName(principal);
      String shortName = name.getShortName();
      System.out.println("Local user = " + shortName);
      return true;
    } catch (Exception e) {
      error("Failed to map principal using auth_to_local rules: "
          + e.getMessage());
      return false;
    }
  }
}

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.security.authentication.util.KerberosName;

/**
 * Validates auth_to_local principal mapping.
 */
public class PrincipalMappingProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Auth-to-Local Mapping";
  }

  @Override
  public ProbeResult test(OzoneConfiguration conf) {

    // Check if krb5.conf is empty or missing ?
    // as it causes KerberosName to fail
    File krbConfig = getKrb5ConfigFile();
    if (!canReadFile(krbConfig, "krb5.conf")) {
      error("Kerberos configuration file (" + krbConfig.getAbsolutePath() +
          ") is missing or empty. Mapping validation cannot proceed.");
      return ProbeResult.FAIL;
    }

    // Read rule
    String rules = conf.getTrimmed(
        "hadoop.security.auth_to_local", "DEFAULT");
    printValue("auth_to_local rules", rules);

    try {
      // Initialise rule
      KerberosName.setRules(rules);
      // Fetch all configured principals
      List<String> principals = getConfiguredPrincipals(conf);
      if (principals.isEmpty()) {
        warn("No Kerberos principals found in configuration to test mapping");
        return ProbeResult.WARN;
      }

      ProbeResult result = ProbeResult.PASS;
      // Validate each principal
      for (String principal : principals) {
        try {
          KerberosName name = new KerberosName(principal);
          String shortName = name.getShortName();
          printValue("Principal = " + principal,
              "to Local user = " + shortName);
        } catch (Exception e) {
          warn("Mapping failed for principal " + principal
              + " : " + e.getMessage());
          // continue checking others
          result = ProbeResult.WARN;
        }
      }
      return result;
    } catch (Exception e) {
      error("auth_to_local mapping check failed: " + e.getMessage());
      return ProbeResult.WARN;
    }
  }

  /**
   * Fetch all configured Kerberos principals from Ozone config.
   */
  private List<String> getConfiguredPrincipals(OzoneConfiguration conf) {

    List<String> principals = new ArrayList<>();

    List<String> principalsFromConfig = Arrays.asList(
        OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY,
        HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY,
        ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY,
        "ozone.s3g.kerberos.principal");

    for (String principal : principalsFromConfig) {
      String value = conf.getTrimmed(principal);
      if (value != null && !value.isEmpty()) {
        principals.add(value);
      }
    }
    return principals;
  }
}

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
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;

/**
 * Validates existence and readability of Ozone service keytab.
 * Reports warnings if keytab are missing or not readable.
 * This probe checks the configured keytab files for major Ozone
 * services such as OM, SCM, recon, s3g and DataNode.
 */
public class KeytabProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Keytab Validation";
  }

  @Override
  public ProbeResult test(OzoneConfiguration conf) {

    // Check if security is enabled
    boolean securityEnabled = Boolean.parseBoolean(
        conf.getTrimmed(OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY));

    if (!securityEnabled) {
      warn("Ozone security is disabled, skipping keytab validation.");
      return ProbeResult.WARN;
    }
    ProbeResult result = ProbeResult.PASS;
    List<String> keys = Arrays.asList(
        OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY,
        ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_KEYTAB_FILE_KEY,
        "ozone.s3g.kerberos.keytab.file" //Used key directly to avoid cyclic dependency
    );

    for (String key : keys) {
      String path = conf.getTrimmed(key);
      if (!checkKeytab(path)) {
        result = ProbeResult.FAIL;
      }
    }
    return result;
  }

  /**
   * Check whether the given keytab exists and is readable.
   */
  private boolean checkKeytab(String path) {

    if (path == null || path.isEmpty()) {
      return true; // not configured hence acceptable
    }
    File file = new File(path);
    if (!file.exists()) {
      warn("keytab missing: " + path);
      return false;
    }
    // Avoid scenario where file does exist but not accessible.
    // Validate actual readability (not just permissions)
    if (!canReadFile(file, "keytab")) {
      return false;
    }
    printValue("Keytab OK", path);
    return true;
  }
}

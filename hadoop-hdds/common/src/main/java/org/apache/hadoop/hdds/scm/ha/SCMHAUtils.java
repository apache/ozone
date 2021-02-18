/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.slf4j.Logger;

import java.util.Collection;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;

/**
 * Utility class used by SCM HA.
 */
public final class SCMHAUtils {
  private SCMHAUtils() {
    // not used
  }

  // Check if SCM HA is enabled.
  public static boolean isSCMHAEnabled(ConfigurationSource conf) {
    return conf.getBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY,
        ScmConfigKeys.OZONE_SCM_HA_ENABLE_DEFAULT);
  }

  /**
   * Get a collection of all scmNodeIds for the given scmServiceId.
   */
  public static Collection<String> getSCMNodeIds(ConfigurationSource conf,
                                                 String scmServiceId) {
    String key = addSuffix(ScmConfigKeys.OZONE_SCM_NODES_KEY, scmServiceId);
    return conf.getTrimmedStringCollection(key);
  }

  public static String  getLocalSCMNodeId(String scmServiceId) {
    return addSuffix(ScmConfigKeys.OZONE_SCM_NODES_KEY, scmServiceId);
  }

  /**
   * Add non empty and non null suffix to a key.
   */
  private static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
        "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }

  /**
   * Get SCM ServiceId from OzoneConfiguration.
   * @param conf
   * @param logger
   * @return SCM service id if defined, else null.
   */
  public static String getScmServiceId(ConfigurationSource conf,
      Logger logger) {

    String localScmServiceId = conf.getTrimmed(
        ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID);

    Collection<String> scmServiceIds;

    if (localScmServiceId == null) {
      // There is no default scm service id is being set, fall back to ozone
      // .scm.service.ids.
      scmServiceIds = conf.getTrimmedStringCollection(
          OZONE_SCM_SERVICE_IDS_KEY);
      if (scmServiceIds.size() > 1) {
        throw new ConfigurationException("When multiple SCM Service Ids are " +
            "configured," + OZONE_SCM_DEFAULT_SERVICE_ID + " need to be " +
            "defined");
      } else if (scmServiceIds.size() == 1) {
        localScmServiceId = scmServiceIds.iterator().next();
      }
    }

    return localScmServiceId;
  }
}

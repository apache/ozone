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

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_DIR;

/**
 * Utility class used by SCM HA.
 */
public final class SCMHAUtils {
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMHAUtils.class);
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
  public static Collection<String> getSCMNodeIds(Configuration conf,
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
   * Get the local directory where ratis logs will be stored.
   */
  public static String getSCMRatisDirectory(ConfigurationSource conf) {
    String scmRatisDirectory =
        conf.getObject(SCMHAConfiguration.class).getRatisStorageDir();

    if (Strings.isNullOrEmpty(scmRatisDirectory)) {
      scmRatisDirectory = ServerUtils.getDefaultRatisDirectory(conf);
    }
    return scmRatisDirectory;
  }

  public static String getSCMRatisSnapshotDirectory(ConfigurationSource conf) {
    String snapshotDir =
        conf.getObject(SCMHAConfiguration.class).getRatisStorageDir();

    // If ratis snapshot directory is not set, fall back to ozone.metadata.dir.
    if (Strings.isNullOrEmpty(snapshotDir)) {
      LOG.warn("SCM snapshot dir is not configured. Falling back to {} config",
          OZONE_METADATA_DIRS);
      File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
      snapshotDir =
          Paths.get(metaDirPath.getPath(), OM_RATIS_SNAPSHOT_DIR).toString();
    }
    return snapshotDir;
  }
}

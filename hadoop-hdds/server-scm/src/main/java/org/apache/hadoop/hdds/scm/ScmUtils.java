/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

import com.google.common.base.Joiner;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmOps;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.safemode.Precheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;

/**
 * SCM utility class.
 */
public final class ScmUtils {
  private static final Logger LOG = LoggerFactory
      .getLogger(ScmUtils.class);

  private ScmUtils() {
  }

  /**
   * Perform all prechecks for given scm operation.
   *
   * @param operation
   * @param preChecks prechecks to be performed
   */
  public static void preCheck(ScmOps operation, Precheck... preChecks)
      throws SCMException {
    for (Precheck preCheck : preChecks) {
      preCheck.check(operation);
    }
  }

  /**
   * Create SCM directory file based on given path.
   */
  public static File createSCMDir(String dirPath) {
    File dirFile = new File(dirPath);
    if (!dirFile.mkdirs() && !dirFile.exists()) {
      throw new IllegalArgumentException("Unable to create path: " + dirFile);
    }
    return dirFile;
  }

  public static Collection<String> getSCMNodeIds(ConfigurationSource conf,
      String scmServiceId) {
    String key = addSuffix(ScmConfigKeys.OZONE_SCM_NODES_KEY, scmServiceId);
    return conf.getTrimmedStringCollection(key);
  }

  private static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
        "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }

  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = concatSuffixes(suffixes);
    return addSuffix(key, keySuffix);
  }

  private static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    return Joiner.on(".").skipNulls().join(suffixes);
  }
}

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

package org.apache.hadoop.ozone.ha;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;

/**
 * Utilities related to configuration.
 */
public final class ConfUtils {

  private ConfUtils() {

  }

  /**
   * Add non empty and non null suffix to a key.
   */
  public static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
        "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }

  /**
   * Return configuration key of format key.suffix1.suffix2...suffixN.
   */
  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = concatSuffixes(suffixes);
    return addSuffix(key, keySuffix);
  }

  /**
   * Concatenate list of suffix strings '.' separated.
   */
  public static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    return Joiner.on(".").skipNulls().join(suffixes);
  }

  /**
   * Get the conf key value appended with serviceId and nodeId.
   * @param conf
   * @param confKey
   * @param omServiceID
   * @param omNodeId
   * @return conf value.
   */
  public static String getConfSuffixedWithServiceId(ConfigurationSource conf,
      String confKey, String omServiceID, String omNodeId) {
    String suffixedConfKey = ConfUtils.addKeySuffixes(
        confKey, omServiceID, omNodeId);
    String confValue = conf.getTrimmed(suffixedConfKey);
    if (StringUtils.isNotEmpty(confValue)) {
      return confValue;
    }
    return null;
  }

  /**
   * Set Node Specific config keys to generic config keys.
   * @param nodeSpecificConfigKeys
   * @param ozoneConfiguration
   * @param serviceId
   * @param nodeId
   */
  public static void setNodeSpecificConfigs(
      String[] nodeSpecificConfigKeys, OzoneConfiguration ozoneConfiguration,
      String serviceId, String nodeId, Logger logger) {
    for (String confKey : nodeSpecificConfigKeys) {
      String confValue = getConfSuffixedWithServiceId(
          ozoneConfiguration, confKey, serviceId, nodeId);
      if (confValue != null) {
        logger.info("Setting configuration key {} with value of key {}: {}",
            confKey, ConfUtils.addKeySuffixes(confKey, serviceId, nodeId),
            confValue);
        ozoneConfiguration.set(confKey, confValue);
      }
    }
  }
}

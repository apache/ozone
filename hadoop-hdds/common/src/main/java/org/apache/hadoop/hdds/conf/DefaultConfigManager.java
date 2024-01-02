/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.conf;

import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to store Default value of configuration keys.
 */
public final class DefaultConfigManager {

  /** Never constructed. **/
  private DefaultConfigManager() {
  }

  private static final Map<String, Object> CONFIG_DEFAULT_MAP = new HashMap<>();

  public static <T> void setConfigValue(String config, T value) {
    T prevValue = getValue(config, value);
    if (!value.equals(prevValue)) {
      throw new ConfigurationException(String.format("Setting conflicting " +
          "Default Configs old default Value: %s New Default Value:%s",
          prevValue.toString(), value.toString()));
    }
    CONFIG_DEFAULT_MAP.putIfAbsent(config, value);
  }
  public static <T> T getValue(String config, T defaultValue) {
    return (T) CONFIG_DEFAULT_MAP.getOrDefault(config, defaultValue);
  }

  public static <T> void forceUpdateConfigValue(String config, T value) {
    CONFIG_DEFAULT_MAP.put(config, value);
  }

  @VisibleForTesting
  public static void clearDefaultConfigs() {
    CONFIG_DEFAULT_MAP.clear();
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;

/**
 * Configuration source to wrap Hadoop Configuration object.
 */
public class LegacyHadoopConfigurationSource
    implements MutableConfigurationSource {

  private Configuration configuration;

  public LegacyHadoopConfigurationSource(
      Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public String get(String key) {
    return configuration.getRaw(key);
  }

  @Override
  public char[] getPassword(String key) throws IOException {
    return configuration.getPassword(key);
  }

  @Override
  public Collection<String> getConfigKeys() {
    return configuration.getPropsWithPrefix("").keySet();
  }

  @Override
  public void set(String key, String value) {
    configuration.set(key, value);
  }

  /**
   * Helper method to get original Hadoop configuration for legacy Hadoop
   * libraries.
   * <p>
   * It can work on server side but not on client side where we might have
   * different configuration.
   */
  public static Configuration asHadoopConfiguration(
      ConfigurationSource config) {
    if (config instanceof Configuration) {
      return (Configuration) config;
    } else if (config instanceof LegacyHadoopConfigurationSource) {
      return ((LegacyHadoopConfigurationSource) config).configuration;
    } else {
      throw new IllegalArgumentException(
          "Core Hadoop code requires real Hadoop configuration");
    }
  }

  public Configuration getOriginalHadoopConfiguration() {
    return configuration;
  }
}

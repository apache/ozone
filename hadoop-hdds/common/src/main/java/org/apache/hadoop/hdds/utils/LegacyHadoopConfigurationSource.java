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

package org.apache.hadoop.hdds.utils;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.DelegatingProperties;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.ozone.OzoneConfigKeys;

/**
 * Configuration source to wrap Hadoop Configuration object.
 */
public class LegacyHadoopConfigurationSource
    implements MutableConfigurationSource {

  private Configuration configuration;

  public LegacyHadoopConfigurationSource(Configuration configuration) {
    this.configuration = new Configuration(configuration) {
      private Properties delegatingProps;

      @Override
      public synchronized void reloadConfiguration() {
        super.reloadConfiguration();
        delegatingProps = null;
      }

      @Override
      protected synchronized Properties getProps() {
        if (delegatingProps == null) {
          String complianceMode = getPropertyUnsafe(OzoneConfigKeys.OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE,
                  OzoneConfigKeys.OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE_UNRESTRICTED);
          Properties cryptoProperties = getCryptoProperties();
          delegatingProps = new DelegatingProperties(super.getProps(), complianceMode, cryptoProperties);
        }
        return delegatingProps;
      }

      /**
       * Get a property value without the compliance check. It's needed to get the compliance mode.
       *
       * @param key property name
       * @param defaultValue default value
       * @return property value, without compliance check
       */
      private String getPropertyUnsafe(String key, String defaultValue) {
        return super.getProps().getProperty(key, defaultValue);
      }

      private Properties getCryptoProperties() {
        try {
          return super.getAllPropertiesByTag(ConfigTag.CRYPTO_COMPLIANCE.toString());
        } catch (NoSuchMethodError e) {
          // We need to handle NoSuchMethodError, because in Hadoop 2 we don't have the
          // getAllPropertiesByTag method. We won't be supporting the compliance mode with
          // that version, so we are safe to catch the exception and return a new Properties object.
          return new Properties();
        }
      }

      @Override
      public Iterator<Map.Entry<String, String>> iterator() {
        DelegatingProperties props = (DelegatingProperties) getProps();
        return props.iterator();
      }
    };
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

  @Override
  public void unset(String key) {
    configuration.unset(key);
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

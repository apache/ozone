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
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE_UNRESTRICTED;

/**
 * Configuration source to wrap Hadoop Configuration object.
 */
public class LegacyHadoopConfigurationSource
    implements MutableConfigurationSource {

  private Configuration configuration;

  @SuppressWarnings("methodlength")
  public LegacyHadoopConfigurationSource(Configuration configuration) {
    this.configuration = new Configuration(configuration) {
      private Properties props;
      private Properties delegatingProps;
      private final String complianceMode =
          getPropertyUnsafe(OzoneConfigKeys.OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE,
              OzoneConfigKeys.OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE_UNRESTRICTED);
      private final boolean checkCompliance =
          !complianceMode.equals(OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE_UNRESTRICTED);
      private final Properties cryptoProperties = getCryptoProperties();

      @Override
      public synchronized void reloadConfiguration() {
        super.reloadConfiguration();
        delegatingProps = null;
        props = null;
      }

      @Override
      protected synchronized Properties getProps() {
        if (delegatingProps == null) {
          props = super.getProps();
          delegatingProps = new Properties() {
            @Override
            public String getProperty(String key) {
              String value = props.getProperty(key);
              return checkCompliance(key, value);
            }

            @Override
            public Object setProperty(String key, String value) {
              return props.setProperty(key, value);
            }

            @Override
            public synchronized Object remove(Object key) {
              return props.remove(key);
            }

            @Override
            public synchronized void clear() {
              props.clear();
            }

            @Override
            public Enumeration<Object> keys() {
              return props.keys();
            }

            @Override
            public Enumeration<?> propertyNames() {
              return props.propertyNames();
            }

            @Override
            public Set<String> stringPropertyNames() {
              return props.stringPropertyNames();
            }

            @Override
            public int size() {
              return props.size();
            }

            @Override
            public boolean isEmpty() {
              return props.isEmpty();
            }

            @Override
            public Set<Object> keySet() {
              return props.keySet();
            }

            @Override
            public boolean contains(Object value) {
              return props.contains(value);
            }

            @Override
            public boolean containsKey(Object key) {
              return props.containsKey(key);
            }

            @Override
            public boolean containsValue(Object value) {
              return props.containsValue(value);
            }

            @Override
            public Object get(Object key) {
              return props.get(key);
            }

            @Override
            public synchronized boolean remove(Object key, Object value) {
              return props.remove(key, value);
            }

            @Override
            public synchronized Object put(Object key, Object value) {
              return props.put(key, value);
            }

            @Override
            public synchronized void putAll(Map<?, ?> t) {
              props.putAll(t);
            }

            @Override
            public Collection<Object> values() {
              return props.values();
            }

            @Override
            public Set<Map.Entry<Object, Object>> entrySet() {
              return props.entrySet();
            }

            @Override
            public synchronized Object clone() {
              return props.clone();
            }
          };
        }
        return delegatingProps;
      }

      /**
       * Get a property value without the compliance check. It's needed to get the compliance
       * mode and the whitelist parameter values in the checkCompliance method.
       *
       * @param key property name
       * @param defaultValue default value
       * @return property value, without compliance check
       */
      private String getPropertyUnsafe(String key, String defaultValue) {
        return super.getProps().getProperty(key, defaultValue);
      }

      private Properties getCryptoProperties() {
        return super.getAllPropertiesByTag(ConfigTag.CRYPTO_COMPLIANCE.toString());
      }

      public String checkCompliance(String config, String value) {
        // Don't check the ozone.security.crypto.compliance.mode config, even though it's tagged as a crypto config
        if (checkCompliance && cryptoProperties.containsKey(config) &&
            !config.equals(OzoneConfigKeys.OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE)) {

          String whitelistConfig = config + "." + complianceMode + ".whitelist";
          String whitelistValue = getPropertyUnsafe(whitelistConfig, "");

          if (whitelistValue != null) {
            String[] whitelistOptions = whitelistValue.split(",");

            if (!Arrays.asList(whitelistOptions).contains(value)) {
              throw new ConfigurationException("Not allowed configuration value! Compliance mode is set to " +
                  complianceMode + " and " + config + " configuration's value is not allowed. Please check the " +
                  whitelistConfig + " configuration.");
            }
          }
        }
        return value;
      }

      @Override
      public Iterator<Map.Entry<String, String>> iterator() {
        Properties props = getProps();
        Map<String, String> result = new HashMap<>();
        synchronized (props) {
          for (Map.Entry<Object, Object> item : props.entrySet()) {
            if (item.getKey() instanceof String && item.getValue() instanceof String) {
              checkCompliance((String) item.getKey(), (String) item.getValue());
              result.put((String) item.getKey(), (String) item.getValue());
            }
          }
        }
        return result.entrySet().iterator();
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

  /**
   *
   * @return
   */
  public Configuration getOriginalHadoopConfiguration() {
    return configuration;
  }
}

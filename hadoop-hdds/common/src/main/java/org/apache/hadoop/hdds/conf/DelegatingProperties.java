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

package org.apache.hadoop.hdds.conf;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE_UNRESTRICTED;

import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.util.StringUtils;

/**
 * Delegating properties helper class. It's needed for configuration related classes, so we are able
 * to delegate the operations that are happening on their Properties object to their parent's
 * Properties object. This is needed because of the configuration compliance checks.
 */
public class DelegatingProperties extends Properties {
  private final Properties properties;
  private final String complianceMode;
  private final boolean checkCompliance;
  private final Properties cryptoProperties;

  public DelegatingProperties(Properties properties, String complianceMode, Properties cryptoProperties) {
    this.properties = properties;
    this.complianceMode = complianceMode;
    this.checkCompliance = !complianceMode.equals(OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE_UNRESTRICTED);
    this.cryptoProperties = cryptoProperties;
  }

  public String checkCompliance(String config, String value) {
    // Don't check the ozone.security.crypto.compliance.mode config, even though it's tagged as a crypto config
    if (checkCompliance && cryptoProperties.containsKey(config) &&
        !config.equals(OzoneConfigKeys.OZONE_SECURITY_CRYPTO_COMPLIANCE_MODE)) {

      String whitelistConfig = config + "." + complianceMode + ".whitelist";
      String whitelistValue = properties.getProperty(whitelistConfig, "");

      if (whitelistValue != null) {
        Collection<String> whitelistOptions = StringUtils.getTrimmedStringCollection(whitelistValue);

        if (!whitelistOptions.contains(value)) {
          throw new ConfigurationException("Not allowed configuration value! Compliance mode is set to " +
              complianceMode + " and " + config + " configuration's value is not allowed. Please check the " +
              whitelistConfig + " configuration.");
        }
      }
    }
    return value;
  }

  @Override
  public String getProperty(String key) {
    String value = properties.getProperty(key);
    return checkCompliance(key, value);
  }

  @Override
  public Object setProperty(String key, String value) {
    return properties.setProperty(key, value);
  }

  @Override
  public synchronized Object remove(Object key) {
    return properties.remove(key);
  }

  @Override
  public synchronized void clear() {
    properties.clear();
  }

  @Override
  public Enumeration<Object> keys() {
    return properties.keys();
  }

  @Override
  public Enumeration<?> propertyNames() {
    return properties.propertyNames();
  }

  @Override
  public Set<String> stringPropertyNames() {
    return properties.stringPropertyNames();
  }

  @Override
  public int size() {
    return properties.size();
  }

  @Override
  public boolean isEmpty() {
    return properties.isEmpty();
  }

  @Override
  public Set<Object> keySet() {
    return properties.keySet();
  }

  @Override
  public boolean contains(Object value) {
    return properties.contains(value);
  }

  @Override
  public boolean containsKey(Object key) {
    return properties.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return properties.containsValue(value);
  }

  @Override
  public Object get(Object key) {
    return properties.get(key);
  }

  @Override
  public synchronized boolean remove(Object key, Object value) {
    return properties.remove(key, value);
  }

  @Override
  public synchronized Object put(Object key, Object value) {
    return properties.put(key, value);
  }

  @Override
  public synchronized void putAll(Map<?, ?> t) {
    properties.putAll(t);
  }

  @Override
  public Collection<Object> values() {
    return properties.values();
  }

  @Override
  public Set<Map.Entry<Object, Object>> entrySet() {
    return properties.entrySet();
  }

  @Override
  public synchronized boolean equals(Object o) {
    return properties.equals(o);
  }

  @Override
  public synchronized int hashCode() {
    return properties.hashCode();
  }

  public Iterator<Map.Entry<String, String>> iterator() {
    Map<String, String> result = new HashMap<>();
    synchronized (properties) {
      for (Map.Entry<Object, Object> item : properties.entrySet()) {
        if (item.getKey() instanceof String && item.getValue() instanceof String) {
          String value = checkCompliance((String) item.getKey(), (String) item.getValue());
          result.put((String) item.getKey(), value);
        }
      }
    }
    return result.entrySet().iterator();
  }
}

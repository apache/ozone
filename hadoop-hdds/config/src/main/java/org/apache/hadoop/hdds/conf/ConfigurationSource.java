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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Defines read-only contract of Configuration objects.
 */
public interface ConfigurationSource {

  String[] EMPTY_STRING_ARRAY = {};

  String get(String key);

  Collection<String> getConfigKeys();

  char[] getPassword(String key) throws IOException;

  default String get(String key, String defaultValue) {
    String value = get(key);
    return value != null ? value : defaultValue;
  }

  default int getInt(String key, int defaultValue) {
    String value = get(key);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  /**
   * Get the value of the <code>name</code> property as a set of comma-delimited
   * <code>int</code> values.
   * <p>
   * If no such property exists, an empty array is returned.
   *
   * @param name property name
   * @return property value interpreted as an array of comma-delimited
   * <code>int</code> values
   */
  default int[] getInts(String name) {
    String[] strings = getTrimmedStrings(name);
    int[] ints = new int[strings.length];
    for (int i = 0; i < strings.length; i++) {
      ints[i] = Integer.parseInt(strings[i]);
    }
    return ints;
  }

  default long getLong(String key, long defaultValue) {
    String value = get(key);
    return value != null ? Long.parseLong(value) : defaultValue;
  }

  default boolean getBoolean(String key, boolean defaultValue) {
    String value = get(key);
    return value != null ? Boolean.parseBoolean(value) : defaultValue;
  }

  default float getFloat(String key, float defaultValue) {
    String value = get(key);
    return value != null ? Float.parseFloat(value) : defaultValue;
  }

  default double getDouble(String key, double defaultValue) {
    String value = get(key);
    return value != null ? Double.parseDouble(value) : defaultValue;
  }

  default String getTrimmed(String key) {
    String value = get(key);
    return value != null ? value.trim() : null;
  }

  default String getTrimmed(String key, String defaultValue) {
    String value = getTrimmed(key);
    return value != null ? value : defaultValue;
  }

  default String[] getTrimmedStrings(String name) {
    String valueString = get(name);
    return getTrimmedStringsFromValue(valueString);
  }

  static String[] getTrimmedStringsFromValue(String valueString) {
    if (null == valueString) {
      return EMPTY_STRING_ARRAY;
    }

    return valueString.trim().split("\\s*[,\n]\\s*");
  }

  /**
   * Gets the configuration entries where the key contains the prefix. This
   * method will strip the prefix from the key in the return Map.
   * Example: {@code somePrefix.key->value} will be {@code key->value} in the returned map.
   * @param keyPrefix Prefix to search.
   * @return Map containing keys that match and their values.
   */
  default Map<String, String> getPropsMatchPrefixAndTrimPrefix(
      String keyPrefix) {
    Map<String, String> configMap = new HashMap<>();
    for (String name : getConfigKeys()) {
      if (name.startsWith(keyPrefix)) {
        String value = get(name);
        String keyName = name.substring(keyPrefix.length());
        configMap.put(keyName, value);
      }
    }
    return configMap;
  }

  /**
   * Gets the configuration entries where the key contains the prefix.
   * This method will return the entire key including the predix in the returned
   * map.
   * @param keyPrefix Prefix to search.
   * @return Map containing keys that match and their values.
   */
  default Map<String, String> getPropsMatchPrefix(String keyPrefix) {
    Map<String, String> configMap = new HashMap<>();
    for (String name : getConfigKeys()) {
      if (name.startsWith(keyPrefix)) {
        String value = get(name);
        configMap.put(name, value);
      }
    }
    return configMap;
  }

  /**
   * Checks if the property <value> is set.
   * @param key The property name.
   * @return true if the value is set else false.
   */
  default boolean isConfigured(String key) {
    return get(key) != null;
  }

  /**
   * Create a Configuration object and inject the required configuration values.
   *
   * @param configurationClass The class where the fields are annotated with
   *                           the configuration.
   * @return Initiated java object where the config fields are injected.
   */
  default <T> T getObject(Class<T> configurationClass) {

    T configObject;

    try {
      configObject = configurationClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException(
          "Configuration class can't be created: " + configurationClass, e);
    }
    ConfigurationReflectionUtil.injectConfiguration(this, configurationClass, configObject, false);

    ConfigurationReflectionUtil.callPostConstruct(configObject);

    return configObject;

  }

  /**
   * Update {@code object}'s reconfigurable properties from this configuration.
   */
  default <T> void reconfigure(Class<T> configClass, T object) {
    ConfigurationReflectionUtil.injectConfiguration(this, configClass, object, true);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Class</code>
   * implementing the interface specified by <code>xface</code>.
   * <p>
   * If no such property is specified, then <code>defaultValue</code> is
   * returned.
   * <p>
   * An exception is thrown if the returned class does not implement the named
   * interface.
   *
   * @param name         the class name.
   * @param defaultValue default value.
   * @param xface        the interface implemented by the named class.
   * @return property value as a <code>Class</code>,
   * or <code>defaultValue</code>.
   */
  default <U> Class<? extends U> getClass(String name,
      Class<? extends U> defaultValue,
      Class<U> xface) {
    try {
      Class<?> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass)) {
        throw new RuntimeException(theClass + " not " + xface.getName());
      } else if (theClass != null) {
        return theClass.asSubclass(xface);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Class</code>.
   * If no such property is specified, then <code>defaultValue</code> is
   * returned.
   *
   * @param name         the class name.
   * @param defaultValue default value.
   * @return property value as a <code>Class</code>,
   * or <code>defaultValue</code>.
   */
  default Class<?> getClass(String name, Class<?> defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null) {
      return defaultValue;
    }
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  default Class<?>[] getClasses(String name, Class<?>... defaultValue) {
    String valueString = get(name);
    if (null == valueString) {
      return defaultValue;
    }
    String[] classnames = getTrimmedStrings(name);
    try {
      Class<?>[] classes = new Class<?>[classnames.length];
      for (int i = 0; i < classnames.length; i++) {
        classes[i] = Class.forName(classnames[i]);
      }
      return classes;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  default long getTimeDuration(String name, long defaultValue,
      TimeUnit unit) {
    String vStr = get(name);
    if (null == vStr) {
      return defaultValue;
    } else {
      return TimeDurationUtil.getTimeDurationHelper(name, vStr, unit);
    }
  }

  default long getTimeDuration(String name, String defaultValue,
      TimeUnit unit) {
    String vStr = get(name);
    if (null == vStr) {
      return TimeDurationUtil.getTimeDurationHelper(name, defaultValue, unit);
    } else {
      return TimeDurationUtil.getTimeDurationHelper(name, vStr, unit);
    }
  }

  default int getBufferSize(String name, String defaultValue) {
    final double size = getStorageSize(name, defaultValue, StorageUnit.BYTES);
    if (size <= 0) {
      throw new IllegalArgumentException(name + " <= 0");
    } else if (size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          name + " > Integer.MAX_VALUE = " + Integer.MAX_VALUE);
    }
    return (int) size;
  }

  default double getStorageSize(String name, String defaultValue,
      StorageUnit targetUnit) {
    String vString = get(name);
    if (vString == null) {
      vString = defaultValue;
    }

    // Please note: There is a bit of subtlety here. If the user specifies
    // the default unit as "1GB", but the requested unit is MB, we will return
    // the format in MB even thought the default string is specified in GB.

    // Converts a string like "1GB" to to unit specified in targetUnit.

    StorageSize measure = StorageSize.parse(vString);

    double byteValue = measure.getUnit().toBytes(measure.getValue());
    return targetUnit.fromBytes(byteValue);
  }

  default Collection<String> getTrimmedStringCollection(String key) {
    return Arrays.asList(getTrimmedStrings(key));
  }

  /**
   * Return value matching this enumerated type.
   * Note that the returned value is trimmed by this method.
   *
   * @param name         Property name
   * @param defaultValue Value returned if no mapping exists
   * @throws IllegalArgumentException If mapping is illegal for the type
   *                                  provided
   */
  default <T extends Enum<T>> T getEnum(String name, T defaultValue) {
    final String val = getTrimmed(name);
    return null == val
        ? defaultValue
        : Enum.valueOf(defaultValue.getDeclaringClass(), val);
  }

}

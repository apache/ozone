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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Reflection utilities for configuration injection.
 */
public final class ConfigurationReflectionUtil {

  private ConfigurationReflectionUtil() {
  }

  public static <T> Map<String, Field> mapReconfigurableProperties(
      Class<T> configurationClass) {
    Map<String, Field> props = new HashMap<>();
    for (Field field : configurationClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(Config.class)) {
        Config configAnnotation = field.getAnnotation(Config.class);

        if (configAnnotation.reconfigurable()) {
          checkNotFinal(configurationClass, field);
          props.put(configAnnotation.key(), field);
        }
      }
    }
    return props;
  }

  public static <T> void injectConfiguration(
      ConfigurationSource from,
      Class<T> configurationClass,
      T configuration,
      boolean reconfiguration) {
    for (Field field : configurationClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(Config.class)) {
        checkNotFinal(configurationClass, field);

        Config configAnnotation = field.getAnnotation(Config.class);

        if (reconfiguration && !configAnnotation.reconfigurable()) {
          continue;
        }

        String key = configAnnotation.key();
        String defaultValue = configAnnotation.defaultValue();
        String value = from.get(key, defaultValue);

        setField(configurationClass, configuration, field, configAnnotation,
            key, value);
      }
    }
  }

  public static <T> void reconfigureProperty(T configuration, Field field,
      String key, String value) {
    Class<?> klass = field.getDeclaringClass();
    if (!field.isAnnotationPresent(Config.class)) {
      throw new ConfigurationException("Not configurable field: "
          + klass + "." + field.getName());
    }
    Config configAnnotation = field.getAnnotation(Config.class);

    try {
      final Object oldValue = forcedFieldGet(field, configuration);
      setField(klass, configuration, field, configAnnotation, key, value);
      try {
        callPostConstruct(configuration);
      } catch (Exception e) {
        forcedFieldSet(field, configuration, oldValue);
        throw e;
      }
    } catch (IllegalAccessException e) {
      throw new ConfigurationException("Failed to inject configuration to "
          + klass.getSimpleName() + "." + field.getName(), e);
    }
  }

  private static <T> void setField(
      Class<?> configurationClass, T configuration, Field field,
      Config configAnnotation, String key, String value) {
    ConfigType type = configAnnotation.type();
    if (type == ConfigType.AUTO) {
      type = detectConfigType(field);
    }

    try {
      Object parsed = type.parse(value, configAnnotation, field.getType(), key);
      forcedFieldSet(field, configuration, parsed);
    } catch (Exception e) {
      throw new ConfigurationException("Failed to inject configuration to "
          + configurationClass.getSimpleName() + "." + field.getName(), e);
    }
  }

  /**
   * Set possibly private {@code field} to {@code value} in {@code object}.
   */
  private static <T> void forcedFieldSet(Field field, T object, Object value)
      throws IllegalAccessException {
    final boolean accessChanged = setAccessible(field);
    try {
      field.set(object, value);
    } finally {
      if (accessChanged) {
        field.setAccessible(false);
      }
    }
  }

  /**
   * @return the value of possibly private {@code field} in {@code object}
   */
  private static <T> Object forcedFieldGet(Field field, T object)
      throws IllegalAccessException {
    final boolean accessChanged = setAccessible(field);
    try {
      return field.get(object);
    } finally {
      if (accessChanged) {
        field.setAccessible(false);
      }
    }
  }

  /**
   * Make {@code field} accessible.
   * @return true if access changed
   */
  private static boolean setAccessible(Field field) {
    if (!field.isAccessible()) {
      field.setAccessible(true);
      return true;
    }
    return false;
  }

  private static ConfigType detectConfigType(Field field) {
    ConfigType type;
    Class<?> parameterType = field.getType();
    if (parameterType == String.class) {
      type = ConfigType.STRING;
    } else if (parameterType == Integer.class || parameterType == int.class) {
      type = ConfigType.INT;
    } else if (parameterType == Long.class || parameterType == long.class) {
      type = ConfigType.LONG;
    } else if (parameterType == Double.class || parameterType == double.class) {
      type = ConfigType.DOUBLE;
    } else if (parameterType == Boolean.class
        || parameterType == boolean.class) {
      type = ConfigType.BOOLEAN;
    } else if (parameterType == Duration.class) {
      type = ConfigType.TIME;
    } else if (parameterType == Class.class) {
      type = ConfigType.CLASS;
    } else {
      throw new ConfigurationException("Unsupported configuration type "
          + parameterType + " in "
          + field.getDeclaringClass() + "." + field.getName());
    }
    return type;
  }

  static <T> void callPostConstruct(T configObject) {
    Class<?> configurationClass = configObject.getClass();
    for (Method method : configurationClass.getMethods()) {
      if (method.isAnnotationPresent(PostConstruct.class)) {
        try {
          method.invoke(configObject);
        } catch (IllegalAccessException ex) {
          throw new IllegalArgumentException(
              "@PostConstruct method in " + configurationClass
                  + " is not accessible");
        } catch (InvocationTargetException e) {
          if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
          } else {
            throw new IllegalArgumentException(
                "@PostConstruct can't be executed on " + configurationClass
                    + " after configObject "
                    + "injection", e);
          }
        }
      }
    }
  }

  public static <T> void updateConfiguration(ConfigurationTarget config, T configObject) {
    Class<?> configClass = configObject.getClass();
    for (Field field : configClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(Config.class)) {
        Config configAnnotation = field.getAnnotation(Config.class);
        String fieldLocation = configClass + "." + field.getName();
        String key = configAnnotation.key();
        ConfigType type = configAnnotation.type();

        if (type == ConfigType.AUTO) {
          type = detectConfigType(field);
        }

        //Note: default value is handled by ozone-default.xml. Here we can
        //use any default.
        try {
          Object value = forcedFieldGet(field, configObject);
          if (value == null) {
            continue;
          }
          type.set(config, key, value, configAnnotation);
        } catch (IllegalAccessException e) {
          throw new ConfigurationException(
              "Can't inject configuration to " + fieldLocation, e);
        }
      }
    }
  }

  public static Optional<String> getDefaultValue(Class<?> configClass,
      String fieldName) {
    return findFieldConfigAnnotationByName(configClass, fieldName)
        .map(Config::defaultValue);
  }

  public static Optional<String> getKey(Class<?> configClass,
      String fieldName) {
    return findFieldConfigAnnotationByName(configClass, fieldName)
        .map(Config::key);
  }

  public static Optional<ConfigType> getType(Class<?> configClass,
      String fieldName) {
    return findFieldConfigAnnotationByName(configClass, fieldName)
        .map(Config::type);
  }

  private static Optional<Config> findFieldConfigAnnotationByName(
      final Class<?> configClass, String fieldName) {
    Class<?> theClass = configClass;
    while (theClass != null) {
      Optional<Config> config = Stream.of(theClass.getDeclaredFields())
          .filter(f -> f.getName().equals(fieldName))
          .findFirst()
          .map(f -> f.getAnnotation(Config.class));

      if (config.isPresent()) {
        return config;
      }

      theClass = theClass.getSuperclass();
      if (Object.class.equals(theClass)) {
        theClass = null;
      }
    }
    return Optional.empty();
  }

  private static void checkNotFinal(
      Class<?> configurationClass, Field field) {

    if ((field.getModifiers() & Modifier.FINAL) != 0) {
      throw new ConfigurationException(String.format(
          "Trying to set final field %s#%s, probably indicates misplaced " +
              "@Config annotation",
          configurationClass.getSimpleName(), field.getName()));
    }
  }
}

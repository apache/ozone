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
package org.apache.hadoop.hdds.conf;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Reflection utilities for configuration injection.
 */
public final class ConfigurationReflectionUtil {

  private ConfigurationReflectionUtil() {
  }

  public static <T> void injectConfiguration(
      ConfigurationSource configuration,
      Class<T> configurationClass,
      T configObject, String prefix) {
    injectConfigurationToObject(configuration, configurationClass, configObject,
        prefix);
    Class<? super T> superClass = configurationClass.getSuperclass();
    while (superClass != null) {
      injectConfigurationToObject(configuration, superClass, configObject,
          prefix);
      superClass = superClass.getSuperclass();
    }
  }

  public static <T> void injectConfigurationToObject(ConfigurationSource from,
      Class<T> configurationClass,
      T configuration,
      String prefix) {
    for (Field field : configurationClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(Config.class)) {
        if ((field.getModifiers() & Modifier.FINAL) != 0) {
          throw new ConfigurationException(String.format(
              "Trying to set final field %s#%s, probably indicates misplaced " +
                  "@Config annotation",
              configurationClass.getSimpleName(), field.getName()));
        }

        String fieldLocation =
            configurationClass + "." + field.getName();

        Config configAnnotation = field.getAnnotation(Config.class);

        String key = prefix + "." + configAnnotation.key();

        String defaultValue = configAnnotation.defaultValue();

        ConfigType type = configAnnotation.type();

        if (type == ConfigType.AUTO) {
          type = detectConfigType(field.getType(), fieldLocation);
        }

        try {
          switch (type) {
          case STRING:
            forcedFieldSet(field, configuration, from.get(key, defaultValue));
            break;
          case INT:
            forcedFieldSet(field, configuration,
                from.getInt(key, Integer.parseInt(defaultValue)));
            break;
          case BOOLEAN:
            forcedFieldSet(field, configuration,
                from.getBoolean(key, Boolean.parseBoolean(defaultValue)));
            break;
          case LONG:
            forcedFieldSet(field, configuration,
                from.getLong(key, Long.parseLong(defaultValue)));
            break;
          case DOUBLE:
            forcedFieldSet(field, configuration,
                from.getDouble(key, Double.parseDouble(defaultValue)));
            break;
          case TIME:
            forcedFieldSet(field, configuration,
                from.getTimeDuration(key, defaultValue,
                    configAnnotation.timeUnit()));
            break;
          case SIZE:
            final long value =
                Math.round(from.getStorageSize(key,
                    defaultValue, StorageUnit.BYTES));
            if (field.getType() == int.class) {
              forcedFieldSet(field, configuration, (int) value);
            } else {
              forcedFieldSet(field, configuration, value);

            }
            break;
          case CLASS:
            forcedFieldSet(field, configuration,
                from.getClass(key, Class.forName(defaultValue)));
            break;
          default:
            throw new ConfigurationException(
                "Unsupported ConfigType " + type + " on " + fieldLocation);
          }
        } catch (IllegalAccessException | ClassNotFoundException e) {
          throw new ConfigurationException(
              "Can't inject configuration to " + fieldLocation, e);
        }

      }
    }
  }

  /**
   * Set the value of one field even if it's private.
   */
  private static <T> void forcedFieldSet(Field field, T object, Object value)
      throws IllegalAccessException {
    boolean accessChanged = false;
    if (!field.isAccessible()) {
      field.setAccessible(true);
      accessChanged = true;
    }
    field.set(object, value);
    if (accessChanged) {
      field.setAccessible(false);
    }
  }

  private static ConfigType detectConfigType(Class<?> parameterType,
      String methodLocation) {
    ConfigType type;
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
    } else if (parameterType == Class.class) {
      type = ConfigType.CLASS;
    } else {
      throw new ConfigurationException(
          "Unsupported configuration type " + parameterType + " in "
              + methodLocation);
    }
    return type;
  }

  public static <T> void callPostConstruct(Class<T> configurationClass,
      T configObject) {
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

  public static <T> void updateConfiguration(ConfigurationTarget config,
      T object, String prefix) {

    Class<?> configClass = object.getClass();
    Deque<Class<?>> classes = new LinkedList<>();
    classes.addLast(configClass);
    Class<?> superclass = configClass.getSuperclass();
    while (superclass != null) {
      classes.addFirst(superclass);
      superclass = superclass.getSuperclass();
    }

    for (Class<?> cl : classes) {
      updateConfigurationFromObject(config, cl, object, prefix);
    }
  }

  private static <T> void updateConfigurationFromObject(
      ConfigurationTarget config, Class<?> configClass, T configObject,
      String prefix) {

    for (Field field : configClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(Config.class)) {
        Config configAnnotation = field.getAnnotation(Config.class);
        String fieldLocation = configClass + "." + field.getName();
        String key = prefix + "." + configAnnotation.key();
        ConfigType type = configAnnotation.type();

        if (type == ConfigType.AUTO) {
          type = detectConfigType(field.getType(), fieldLocation);
        }

        //Note: default value is handled by ozone-default.xml. Here we can
        //use any default.
        boolean accessChanged = false;
        try {
          if (!field.isAccessible()) {
            field.setAccessible(true);
            accessChanged = true;
          }
          switch (type) {
          case STRING:
            Object value = field.get(configObject);
            if (value != null) {
              config.set(key, String.valueOf(value));
            }
            break;
          case INT:
            config.setInt(key, field.getInt(configObject));
            break;
          case BOOLEAN:
            config.setBoolean(key, field.getBoolean(configObject));
            break;
          case LONG:
            config.setLong(key, field.getLong(configObject));
            break;
          case DOUBLE:
            config.setDouble(key, field.getDouble(configObject));
            break;
          case TIME:
            config.setTimeDuration(key, field.getLong(configObject),
                configAnnotation.timeUnit());
            break;
          case SIZE:
            config.setStorageSize(key, field.getLong(configObject),
                StorageUnit.BYTES);
            break;
          case CLASS:
            Object valueClass = field.get(configObject);
            if (valueClass instanceof Class<?>) {
              config.set(key, ((Class<?>) valueClass).getName());
            }
            break;
          default:
            throw new ConfigurationException(
                "Unsupported ConfigType " + type + " on " + fieldLocation);
          }
        } catch (IllegalAccessException e) {
          throw new ConfigurationException(
              "Can't inject configuration to " + fieldLocation, e);
        } finally {
          if (accessChanged) {
            field.setAccessible(false);
          }
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
    ConfigGroup configGroup =
        configClass.getAnnotation(ConfigGroup.class);

    return findFieldConfigAnnotationByName(configClass,
        fieldName).map(
            config -> configGroup == null ? config.key()
                : configGroup.prefix() + "." + config.key());
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
}

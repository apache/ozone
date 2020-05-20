/**
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

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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

        String fieldLocation =
            configurationClass + "." + field.getName();

        Config configAnnotation = field.getAnnotation(Config.class);

        String key = prefix + "." + configAnnotation.key();

        ConfigType type = configAnnotation.type();

        if (type == ConfigType.AUTO) {
          type = detectConfigType(field.getType(), fieldLocation);
        }

        //Note: default value is handled by ozone-default.xml. Here we can
        //use any default.
        try {
          switch (type) {
          case STRING:
            forcedFieldSet(field, configuration, from.get(key));
            break;
          case INT:
            forcedFieldSet(field, configuration, from.getInt(key, 0));
            break;
          case BOOLEAN:
            forcedFieldSet(field, configuration, from.getBoolean(key, false));
            break;
          case LONG:
            forcedFieldSet(field, configuration, from.getLong(key, 0));
            break;
          case TIME:
            forcedFieldSet(field, configuration,
                from.getTimeDuration(key, "0s", configAnnotation.timeUnit()));
            break;
          case SIZE:
            forcedFieldSet(field, configuration,
                from.getStorageSize(key, "0B", configAnnotation.sizeUnit()));
            break;
          default:
            throw new ConfigurationException(
                "Unsupported ConfigType " + type + " on " + fieldLocation);
          }
        } catch (IllegalAccessException e) {
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
    } else if (parameterType == Boolean.class
        || parameterType == boolean.class) {
      type = ConfigType.BOOLEAN;
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
}

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
package org.apache.hadoop.hdds.conf.guice;

import java.lang.reflect.Field;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigurationReflectionUtil;
import org.apache.hadoop.hdds.conf.ConfigurationSource;

import com.google.inject.MembersInjector;
import com.google.inject.TypeLiteral;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

/**
 * COnfiguration injection listener for Guice modiles.
 */
public class ConfigInjector implements TypeListener {

  private ConfigurationSource configurationSource;

  public ConfigInjector(ConfigurationSource configurationSource) {
    this.configurationSource = configurationSource;
  }

  @Override
  public <I> void hear(
      TypeLiteral<I> type, TypeEncounter<I> encounter
  ) {
    Class<?> clazz = type.getRawType();
    final ConfigGroup configGroup = clazz.getAnnotation(ConfigGroup.class);
    if (configGroup == null) {
      return;
    }

    while (clazz != null) {
      for (Field field : clazz.getDeclaredFields()) {

        if (field.isAnnotationPresent(Config.class)) {

          Class<?> finalClazz = clazz;

          encounter.register((MembersInjector<I>) instance -> {

            ConfigurationReflectionUtil.injectField(
                configurationSource,
                (Class<I>) finalClazz.getClass(),
                instance,
                configGroup.prefix(),
                field);
          });
        }
      }
      clazz = clazz.getSuperclass();
    }
  }
}

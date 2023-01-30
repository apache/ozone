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

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.TimeDurationUtil.ParsedTimeDuration;

/**
 * Defines write contract of Configuration objects.
 */
public interface ConfigurationTarget {

  void set(String key, String value);

  default void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }

  default void setLong(String name, long value) {
    set(name, Long.toString(value));
  }

  default void setDouble(String name, double value) {
    set(name, Double.toString(value));
  }

  default void setBoolean(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  default void setTimeDuration(String name, long value, TimeUnit unit) {
    set(name, value + ParsedTimeDuration.unitFor(unit).suffix());
  }

  default void setStorageSize(String name, long value, StorageUnit unit) {
    set(name, value + unit.getShortName());
  }

  default <T> void setFromObject(T object) {
    ConfigGroup configGroup =
        object.getClass().getAnnotation(ConfigGroup.class);
    String prefix = configGroup.prefix();
    ConfigurationReflectionUtil.updateConfiguration(this, object, prefix);
  }

}

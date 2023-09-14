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
import java.util.Map;
import java.util.Set;

/**
 * Base class for config with reconfigurable properties.
 */
public abstract class ReconfigurableConfig {

  private final Map<String, Field> reconfigurableProperties =
      ConfigurationReflectionUtil.mapReconfigurableProperties(getClass());

  public void reconfigureProperty(String property, String newValue) {
    Field field = reconfigurableProperties.get(property);
    if (field != null) {
      ConfigurationReflectionUtil.reconfigureProperty(this,
          field, property, newValue);
    }
  }

  public Set<String> reconfigurableProperties() {
    return reconfigurableProperties.keySet();
  }
}

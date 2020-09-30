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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * In memory, mutable configuration source for testing.
 */
public class InMemoryConfiguration implements MutableConfigurationSource {

  private Map<String, String> configs = new HashMap<>();

  public InMemoryConfiguration() {
  }

  public InMemoryConfiguration(String key, String value) {
    set(key, value);
  }

  @Override
  public String get(String key) {
    return configs.get(key);
  }

  @Override
  public Collection<String> getConfigKeys() {
    return configs.keySet();
  }

  @Override
  public char[] getPassword(String key) throws IOException {
    return configs.get(key).toCharArray();
  }

  @Override
  public void set(String key, String value) {
    configs.put(key, value);
  }
}

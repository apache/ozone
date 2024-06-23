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

import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class DelegatingProperties extends Properties {
  private final OzoneConfiguration entries;
  private final Properties
      properties;

  public DelegatingProperties(OzoneConfiguration entries, Properties properties) {
    this.entries = entries;
    this.properties = properties;
  }

  @Override
  public String getProperty(String key) {
    String value = properties.getProperty(key);
    return entries.checkCompliance(key, value);
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
}

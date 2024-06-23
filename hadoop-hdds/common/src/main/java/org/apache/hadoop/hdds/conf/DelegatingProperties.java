package org.apache.hadoop.hdds.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class DelegatingProperties extends Properties {
  private final OzoneConfiguration entries;
  private Properties properties;

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
  public synchronized Object clone() {
    return properties.clone();
  }
}

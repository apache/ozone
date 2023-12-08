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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.ratis.util.function.CheckedConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

import static java.util.Collections.unmodifiableSet;
import static java.util.function.UnaryOperator.identity;

/**
 * Keeps track of reconfigurable properties and the corresponding functions
 * that implement reconfiguration.
 */
public class ReconfigurationHandler extends ReconfigurableBase
    implements ReconfigureProtocol {

  private final String name;
  private final CheckedConsumer<String, IOException> requireAdminPrivilege;
  private final Map<String, UnaryOperator<String>> properties =
      new ConcurrentHashMap<>();

  public ReconfigurationHandler(String name, OzoneConfiguration config,
      CheckedConsumer<String, IOException> requireAdminPrivilege) {
    super(config);
    this.name = name;
    this.requireAdminPrivilege = requireAdminPrivilege;
  }

  public ReconfigurationHandler register(
      String property, UnaryOperator<String> reconfigureFunction) {
    properties.put(property, reconfigureFunction);
    return this;
  }

  public ReconfigurationHandler register(ReconfigurableConfig config) {
    config.reconfigurableProperties().forEach(
        prop -> properties.put(prop, newValue -> {
          config.reconfigureProperty(prop, newValue);
          return newValue;
        })
    );
    return this;
  }

  @Override
  protected Configuration getNewConf() {
    return new OzoneConfiguration();
  }

  @Override
  public Set<String> getReconfigurableProperties() {
    return unmodifiableSet(properties.keySet());
  }

  @Override
  public String reconfigurePropertyImpl(String property, String newValue)
      throws ReconfigurationException {
    final String oldValue = getConf().get(property);
    try {
      return properties.getOrDefault(property, identity())
          .apply(newValue);
    } catch (Exception e) {
      throw new ReconfigurationException(property, newValue, oldValue, e);
    }
  }

  @Override
  public String getServerName() {
    return name;
  }

  @Override
  public void startReconfigure() throws IOException {
    requireAdminPrivilege.accept("startReconfiguration");
    startReconfigurationTask();
  }

  @Override
  public ReconfigurationTaskStatus getReconfigureStatus() throws IOException {
    requireAdminPrivilege.accept("getReconfigurationStatus");
    return getReconfigurationTaskStatus();
  }

  @Override
  public List<String> listReconfigureProperties() throws IOException {
    requireAdminPrivilege.accept("listReconfigurableProperties");
    return new ArrayList<>(new TreeSet<>(getReconfigurableProperties()));
  }

  @Override
  public void close() throws IOException {
    shutdownReconfigurationTask();
  }
}

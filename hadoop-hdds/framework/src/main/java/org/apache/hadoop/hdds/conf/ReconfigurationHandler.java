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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

import static java.util.function.UnaryOperator.identity;

/**
 * Keeps track of reconfigurable properties and the corresponding functions
 * that implement reconfiguration.
 */
public class ReconfigurationHandler extends ReconfigurableBase {

  private final Map<String, UnaryOperator<String>> properties =
      new ConcurrentHashMap<>();

  public ReconfigurationHandler(OzoneConfiguration config) {
    setConf(config);
  }

  public ReconfigurationHandler register(
      String property, UnaryOperator<String> reconfigureFunction) {
    properties.put(property, reconfigureFunction);
    return this;
  }

  @Override
  protected Configuration getNewConf() {
    return new OzoneConfiguration();
  }

  @Override
  public List<String> getReconfigurableProperties() {
    return new ArrayList<>(new TreeSet<>(properties.keySet()));
  }

  @Override
  public String reconfigurePropertyImpl(String property, String newValue) {
    return properties.getOrDefault(property, identity())
        .apply(newValue);
  }
}

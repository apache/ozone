/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.conf;

import static java.util.Collections.unmodifiableSet;
import static java.util.function.UnaryOperator.identity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.ratis.util.function.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of reconfigurable properties and the corresponding functions
 * that implement reconfiguration.
 */
public class ReconfigurationHandler extends ReconfigurableBase
    implements ReconfigureProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(ReconfigurationHandler.class);
  private final String name;
  private final CheckedConsumer<String, IOException> requireAdminPrivilege;
  private final Map<String, UnaryOperator<String>> properties =
      new ConcurrentHashMap<>();

  private final List<ReconfigurationChangeCallback> completeCallbacks = new ArrayList<>();
  private BiConsumer<ReconfigurationTaskStatus, Configuration> reconfigurationStatusListener;

  public void registerCompleteCallback(ReconfigurationChangeCallback callback) {
    completeCallbacks.add(callback);
  }

  public void setReconfigurationCompleteCallback(BiConsumer<ReconfigurationTaskStatus, Configuration>
      statusListener) {
    this.reconfigurationStatusListener = statusListener;
  }

  public BiConsumer<ReconfigurationTaskStatus, Configuration> defaultLoggingCallback() {
    return (status, conf) -> {
      if (status.getStatus() != null && !status.getStatus().isEmpty()) {
        LOG.info("Reconfiguration completed with {} updated properties.",
            status.getStatus().size());
      } else {
        LOG.info("Reconfiguration complete. No properties were changed.");
      }
    };
  }

  private void triggerCompleteCallbacks(ReconfigurationTaskStatus status, Configuration newConf) {
    if (status.getStatus() != null && !status.getStatus().isEmpty()) {
      Map<String, Boolean> changedKeys = new HashMap<>();
      for (ReconfigurationUtil.PropertyChange change : status.getStatus().keySet()) {
        boolean deleted = change.newVal == null;
        changedKeys.put(change.prop, !deleted);
      }
      for (ReconfigurationChangeCallback callback : completeCallbacks) {
        callback.onPropertiesChanged(changedKeys, newConf);
      }
    }

    if (reconfigurationStatusListener != null) {
      reconfigurationStatusListener.accept(status, newConf);
    }
  }

  public ReconfigurationHandler(String name, OzoneConfiguration config,
      CheckedConsumer<String, IOException> requireAdminPrivilege) {
    super(config);
    this.name = name;
    this.requireAdminPrivilege = requireAdminPrivilege;

    // Register callback on reconfiguration complete
    addReconfigurationCompleteCallback(status -> {
      Configuration newConf = getNewConf();
      triggerCompleteCallbacks(status, newConf);
    });

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

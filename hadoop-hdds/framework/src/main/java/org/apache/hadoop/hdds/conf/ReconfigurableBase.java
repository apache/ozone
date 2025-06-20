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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.hadoop.conf.ConfigRedactor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Reconfigurable;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class to support dynamic reconfiguration of configuration properties at runtime.
 */
public abstract class ReconfigurableBase extends Configured implements Reconfigurable {
  private static final Logger LOG = LoggerFactory.getLogger(ReconfigurableBase.class);
  private Thread reconfigThread = null;
  private volatile boolean shouldRun = true;
  private final Object reconfigLock = new Object();
  private long startTime = 0L;
  private long endTime = 0L;
  private Map<ReconfigurationUtil.PropertyChange, Optional<String>> status = null;
  private final Collection<Consumer<ReconfigurationTaskStatus>> reconfigurationCompleteCallbacks = new ArrayList<>();

  public ReconfigurableBase(Configuration conf) {
    super(conf == null ? new Configuration() : conf);
  }

  protected abstract Configuration getNewConf();

  public void startReconfigurationTask() throws IOException {
    synchronized (this.reconfigLock) {
      String errorMessage;
      if (!this.shouldRun) {
        errorMessage = "The server is stopped.";
        LOG.warn(errorMessage);
        throw new IOException(errorMessage);
      } else if (this.reconfigThread != null) {
        errorMessage = "Another reconfiguration task is running.";
        LOG.warn(errorMessage);
        throw new IOException(errorMessage);
      } else {
        this.reconfigThread = new ReconfigurationThread(this);
        this.reconfigThread.setDaemon(true);
        this.reconfigThread.setName("Reconfiguration Task");
        this.reconfigThread.start();
        this.startTime = Time.now();
      }
    }
  }

  public ReconfigurationTaskStatus getReconfigurationTaskStatus() {
    synchronized (this.reconfigLock) {
      return this.reconfigThread != null ? new ReconfigurationTaskStatus(this.startTime, 0L, null) :
          new ReconfigurationTaskStatus(this.startTime, this.endTime, this.status);
    }
  }

  public void shutdownReconfigurationTask() {
    Thread tempThread;
    synchronized (this.reconfigLock) {
      this.shouldRun = false;
      if (this.reconfigThread == null) {
        return;
      }

      tempThread = this.reconfigThread;
      this.reconfigThread = null;
    }

    try {
      tempThread.join();
    } catch (InterruptedException ignored) {
    }

  }

  @Override
  public final void reconfigureProperty(String property, String newVal) throws ReconfigurationException {
    if (this.isPropertyReconfigurable(property)) {
      LOG.info("changing property " + property + " to " + newVal);
      synchronized (this.getConf()) {
        this.getConf().get(property);
        String effectiveValue = this.reconfigurePropertyImpl(property, newVal);
        if (newVal != null) {
          this.getConf().set(property, effectiveValue);
        } else {
          this.getConf().unset(property);
        }
      }
    } else {
      throw new ReconfigurationException(property, newVal, this.getConf().get(property));
    }
  }

  @Override
  public abstract Collection<String> getReconfigurableProperties();

  @Override
  public boolean isPropertyReconfigurable(String property) {
    return this.getReconfigurableProperties().contains(property);
  }

  protected abstract String reconfigurePropertyImpl(String var1, String var2) throws ReconfigurationException;

  private static class ReconfigurationThread extends Thread {
    private final ReconfigurableBase parent;

    ReconfigurationThread(ReconfigurableBase base) {
      this.parent = base;
    }

    @Override
    public void run() {
      LOG.info("Starting reconfiguration task.");
      Configuration oldConf = this.parent.getConf();
      Configuration newConf = this.parent.getNewConf();
      Collection<ReconfigurationUtil.PropertyChange> changes =
          ReconfigurationUtil.getChangedProperties(newConf, oldConf);
      Map<ReconfigurationUtil.PropertyChange, Optional<String>> results = Maps.newHashMap();
      ConfigRedactor oldRedactor = new ConfigRedactor(oldConf);
      ConfigRedactor newRedactor = new ConfigRedactor(newConf);

      for (ReconfigurationUtil.PropertyChange change : changes) {
        String errorMessage = null;
        String oldValRedacted = oldRedactor.redact(change.prop, change.oldVal);
        String newValRedacted = newRedactor.redact(change.prop, change.newVal);
        if (!this.parent.isPropertyReconfigurable(change.prop)) {
          LOG.info(String.format("Property %s is not configurable: old value: %s, new value: %s",
              change.prop, oldValRedacted, newValRedacted));
        } else {
          LOG.info("Change property: " + change.prop + " from \"" +
              (change.oldVal == null ? "<default>" : oldValRedacted) + "\" to \"" +
              (change.newVal == null ? "<default>" : newValRedacted) + "\".");

          try {
            String effectiveValue = this.parent.reconfigurePropertyImpl(change.prop, change.newVal);
            if (change.newVal != null) {
              oldConf.set(change.prop, effectiveValue);
            } else {
              oldConf.unset(change.prop);
            }
          } catch (ReconfigurationException reconfException) {
            Throwable cause = reconfException.getCause();
            errorMessage = cause == null ? reconfException.getMessage() : cause.getMessage();
            LOG.error("Failed to reconfigure property {}: {}", change.prop, errorMessage, reconfException);
          }

          results.put(change, Optional.ofNullable(errorMessage));
        }
      }

      synchronized (this.parent.reconfigLock) {
        this.parent.endTime = Time.now();
        this.parent.status = Collections.unmodifiableMap(results);
        this.parent.reconfigThread = null;

        for (Consumer<ReconfigurationTaskStatus> callback : parent.reconfigurationCompleteCallbacks) {
          try {
            callback.accept(parent.getReconfigurationTaskStatus());
          } catch (Exception e) {
            LOG.warn("Reconfiguration complete callback threw exception", e);
          }
        }
      }
    }
  }

  public void addReconfigurationCompleteCallback(Consumer<ReconfigurationTaskStatus> callback) {
    synchronized (reconfigLock) {
      this.reconfigurationCompleteCallbacks.add(callback);
    }
  }

}

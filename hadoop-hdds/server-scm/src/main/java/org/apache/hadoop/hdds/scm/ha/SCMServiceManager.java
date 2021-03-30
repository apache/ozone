/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.scm.ha.SCMService.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Manipulate background services in SCM, including ReplicationManager,
 * SCMBlockDeletingService and BackgroundPipelineCreator.
 */
public final class SCMServiceManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMServiceManager.class);

  private final List<SCMService> services = new ArrayList<>();

  /**
   * Register a SCMService to SCMServiceManager.
   */
  public synchronized void register(SCMService service) {
    Preconditions.checkNotNull(service);
    LOG.info("Registering service {}.", service.getServiceName());
    services.add(service);
  }

  /**
   * Notify raft or safe mode related status changed.
   */
  public synchronized void notifyStatusChanged() {
    for (SCMService service : services) {
      LOG.debug("Notify service:{}.", service.getServiceName());
      service.notifyStatusChanged();
    }
  }

  /**
   * Notify event triggered, which may affect SCMService.
   */
  public synchronized void notifyEventTriggered(Event event) {
    for (SCMService service : services) {
      LOG.debug("Notify service:{} with event:{}.",
          service.getServiceName(), event);
      service.notifyEventTriggered(event);
    }
  }

  /**
   * Starts all running services.
   */
  public synchronized void start() {
    for (SCMService service : services) {
      LOG.debug("Stopping service:{}.", service.getServiceName());
      service.start();
    }
  }

  /**
   * Stops all running services.
   */
  public synchronized void stop() {
    for (SCMService service : services) {
      LOG.debug("Stopping service:{}.", service.getServiceName());
      service.stop();
    }
  }
}

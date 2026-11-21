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

package org.apache.hadoop.hdds.scm.simulation;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hadoop.ozone.lease.Lease;
import org.apache.hadoop.ozone.lease.LeaseAlreadyExistException;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.lease.LeaseNotFoundException;

/**
 * LeaseManager whose leases expire on the simulated clock. Only the calls used by CloseContainerEventHandler are
 * supported; it is never started, so it has no monitor thread.
 */
final class SimLeaseManager extends LeaseManager<Object> {

  private final SimScheduler scheduler;
  private final Map<Object, SimScheduler.Timer> active = new HashMap<>();

  SimLeaseManager(SimScheduler scheduler, long defaultTimeout) {
    super("Sim", defaultTimeout);
    this.scheduler = scheduler;
  }

  @Override
  public Lease<Object> acquire(Object resource, long timeout, Callable<Void> callback)
      throws LeaseAlreadyExistException {
    if (active.containsKey(resource)) {
      throw new LeaseAlreadyExistException("Resource: " + resource);
    }
    active.put(resource, scheduler.schedule("lease-expired " + resource.getClass().getSimpleName(), timeout, () -> {
      active.remove(resource);
      try {
        callback.call();
      } catch (Exception e) {
        throw new IllegalStateException("Lease callback failed", e);
      }
    }));
    return new Lease<>(resource, timeout, callback);
  }

  @Override
  public void release(Object resource) throws LeaseNotFoundException {
    SimScheduler.Timer timer = active.remove(resource);
    if (timer == null) {
      throw new LeaseNotFoundException("Resource: " + resource);
    }
    scheduler.cancel(timer);
  }

  @Override
  public void shutdown() {
    active.values().forEach(scheduler::cancel);
    active.clear();
  }
}

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

package org.apache.hadoop.ozone.lease;

import static org.apache.hadoop.ozone.lease.Lease.messageForResource;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LeaseManager is someone who can provide you leases based on your
 * requirement. If you want to return the lease back before it expires,
 * you can give it back to Lease Manager. He is the one responsible for
 * the lifecycle of leases. The resource for which lease is created
 * should have proper {@code equals} method implementation, resource
 * equality is checked while the lease is created.
 *
 * @param <T> Type of leases that this lease manager can create
 */
public class LeaseManager<T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(LeaseManager.class);

  private final String name;
  private final long defaultTimeout;
  private Map<T, Lease<T>> activeLeases;
  private Semaphore semaphore = new Semaphore(0);
  private LeaseMonitor leaseMonitor;
  private Thread leaseMonitorThread;
  private boolean isRunning;

  /**
   * Creates an instance of lease manager.
   *
   * @param name
   *        Name for the LeaseManager instance.
   * @param defaultTimeout
   *        Default timeout in milliseconds to be used for lease creation.
   */
  public LeaseManager(String name, long defaultTimeout) {
    this.name = name + "LeaseManager";
    this.defaultTimeout = defaultTimeout;
  }

  /**
   * Starts the lease manager service.
   */
  public void start() {
    LOG.debug("Starting {} service", name);
    activeLeases = new ConcurrentHashMap<>();
    leaseMonitor = new LeaseMonitor();
    leaseMonitorThread = new Thread(leaseMonitor);
    leaseMonitorThread.setName(name + "#LeaseMonitor");
    leaseMonitorThread.setDaemon(true);
    leaseMonitorThread.setUncaughtExceptionHandler((thread, throwable) -> {
      // Let us just restart this thread after logging an error.
      // if this thread is not running we cannot handle Lease expiry.
      LOG.error("LeaseMonitor thread encountered an error. Thread: {}",
          thread.toString(), throwable);
      leaseMonitorThread.start();
    });
    LOG.debug("Starting {} Thread", leaseMonitorThread.getName());
    leaseMonitorThread.start();
    isRunning = true;
  }

  /**
   * Returns a lease for the specified resource with default timeout.
   *
   * @param resource
   *        Resource for which lease has to be created
   * @throws LeaseAlreadyExistException
   *         If there is already a lease on the resource
   */
  public synchronized Lease<T> acquire(T resource)
      throws LeaseAlreadyExistException {
    return acquire(resource, defaultTimeout);
  }

  /**
   * Returns a lease for the specified resource with the timeout provided.
   *
   * @param resource
   *        Resource for which lease has to be created
   * @param timeout
   *        The timeout in milliseconds which has to be set on the lease
   * @throws LeaseAlreadyExistException
   *         If there is already a lease on the resource
   */
  public synchronized Lease<T> acquire(T resource, long timeout)
      throws LeaseAlreadyExistException {
    checkStatus();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Acquiring lease on {} for {} milliseconds", resource, timeout);
    }
    if (activeLeases.containsKey(resource)) {
      throw new LeaseAlreadyExistException(messageForResource(resource));
    }
    Lease<T> lease = new Lease<>(resource, timeout);
    activeLeases.put(resource, lease);
    semaphore.release();
    return lease;
  }

  /**
   * Returns a lease for the specified resource with the timeout provided.
   *
   * @param resource
   *        Resource for which lease has to be created
   * @param timeout
   *        The timeout in milliseconds which has to be set on the lease
   * @param callback
   *        The callback trigger when lease expire
   * @throws LeaseAlreadyExistException
   *         If there is already a lease on the resource
   */
  public synchronized Lease<T> acquire(
      T resource, long timeout, Callable<Void> callback)
      throws LeaseAlreadyExistException, LeaseExpiredException {
    checkStatus();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Acquiring lease on {} for {} milliseconds", resource, timeout);
    }
    if (activeLeases.containsKey(resource)) {
      throw new LeaseAlreadyExistException(messageForResource(resource));
    }
    Lease<T> lease = new Lease<>(resource, timeout, callback);
    activeLeases.put(resource, lease);
    semaphore.release();
    return lease;
  }

  /**
   * Returns a lease for the specified resource with the default timeout.
   *
   * @param resource
   *        Resource for which lease has to be created
   * @param callback
   *        The callback trigger when lease expire
   * @throws LeaseAlreadyExistException
   *         If there is already a lease on the resource
   */
  public synchronized Lease<T> acquire(
      T resource, Callable<Void> callback)
      throws LeaseAlreadyExistException, LeaseExpiredException {
    return acquire(resource, defaultTimeout, callback);
  }

  /**
   * Returns a lease associated with the specified resource.
   *
   * @param resource
   *        Resource for which the lease has to be returned
   * @throws LeaseNotFoundException
   *         If there is no active lease on the resource
   */
  public Lease<T> get(T resource) throws LeaseNotFoundException {
    checkStatus();
    Lease<T> lease = activeLeases.get(resource);
    if (lease != null) {
      return lease;
    }
    throw new LeaseNotFoundException(messageForResource(resource));
  }

  /**
   * Releases the lease associated with the specified resource.
   *
   * @param resource
   *        The for which the lease has to be released
   * @throws LeaseNotFoundException
   *         If there is no active lease on the resource
   */
  public synchronized void release(T resource)
      throws LeaseNotFoundException {
    checkStatus();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Releasing lease on {}", resource);
    }
    Lease<T> lease = activeLeases.remove(resource);
    if (lease == null) {
      throw new LeaseNotFoundException(messageForResource(resource));
    }
    lease.invalidate();
  }

  /**
   * Shuts down the LeaseManager and releases the resources. All the active
   * {@link Lease} will be released (callbacks on leases will not be
   * executed).
   */
  public void shutdown() {
    checkStatus();
    LOG.debug("Shutting down LeaseManager service");
    leaseMonitor.disable();
    // added extra release for case when interrupt is called
    // before going to semaphore's tryAcquire. This will ensure release
    //  of wait and exit of while loop as leaseMonitor.disable() is done.
    semaphore.release();
    leaseMonitorThread.interrupt();
    for (T resource : activeLeases.keySet()) {
      try {
        release(resource);
      } catch (LeaseNotFoundException ex) {
        //Ignore the exception, someone might have released the lease
      }
    }
    isRunning = false;
  }

  /**
   * Throws {@link LeaseManagerNotRunningException} if the service is not
   * running.
   */
  private void checkStatus() {
    if (!isRunning) {
      throw new LeaseManagerNotRunningException("LeaseManager not running.");
    }
  }

  /**
   * Monitors the leases and expires them based on the timeout, also
   * responsible for executing the callbacks of expired leases.
   */
  private final class LeaseMonitor implements Runnable {

    private final ExecutorService executorService;
    private volatile boolean running = true;

    private LeaseMonitor() {
      this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void run() {
      while (running) {
        LOG.debug("{}-LeaseMonitor: checking for lease expiry", name);
        long sleepTime = Long.MAX_VALUE;

        for (T resource : activeLeases.keySet()) {
          try {
            Lease<T> lease = get(resource);
            long remainingTime = lease.getRemainingTime();
            if (remainingTime <= 0) {
              //Lease has timed out
              Callable<Void> leaseCallback = lease.getCallback();
              release(resource);
              executorService.execute(
                  new LeaseCallbackExecutor<>(resource, leaseCallback));
            } else {
              sleepTime = Math.min(remainingTime, sleepTime);
            }
          } catch (LeaseNotFoundException | LeaseExpiredException ex) {
            //Ignore the exception, someone might have released the lease
          }
        }

        try {
          // ignore return value, just used for wait
          boolean ignored = semaphore.tryAcquire(sleepTime, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOG.warn("Lease manager is interrupted. Shutting down...", e);
          Thread.currentThread().interrupt();
        }
      }
    }

    /**
     * Disables lease monitor, next interrupt call on the thread
     * will stop lease monitor.
     */
    public void disable() {
      running = false;
    }
  }

}

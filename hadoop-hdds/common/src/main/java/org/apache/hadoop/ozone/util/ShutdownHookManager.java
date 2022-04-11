/**
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

package org.apache.hadoop.ozone.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.OZONE_SHUTDOWN_TIMEOUT_MINIMUM;
import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.OZONE_SHUTDOWN_TIME_UNIT_DEFAULT;

/**
 * The <code>ShutdownHookManager</code> enables running shutdownHook
 * in a deterministic order, higher priority first.
 * <p/>
 * The JVM runs ShutdownHooks in a non-deterministic order or in parallel.
 * This class registers a single JVM shutdownHook and run all the
 * shutdownHooks registered to it (to this class) in order based on their
 * priority.
 *
 * Unless a hook was registered with a shutdown explicitly set through
 * {@link #addShutdownHook(Runnable, int, long, TimeUnit)},
 * the shutdown time allocated to it is set by the configuration option
 * {@link org.apache.hadoop.ozone.conf.OzoneServiceConfig
 * #SERVICE_SHUTDOWN_TIMEOUT}
 * {@code ozone-site.xml}, with a default value of
 * {@link org.apache.hadoop.ozone.conf.OzoneServiceConfig
 * #SERVICE_SHUTDOWN_TIMEOUT_DEFAULT}
 * seconds.
 *
 * This code is taken from hadoop project.
 * 1. This is to avoid dependency on hadoop.
 * 2. To use a ozone specific config and defaults.
 * 3. Now any fix happened to this class in hadoop for this class, we can
 * backport this to this with out waiting for a new hadoop release.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ShutdownHookManager {

  private static final ShutdownHookManager MGR = new ShutdownHookManager();

  private static final Logger LOG =
      LoggerFactory.getLogger(ShutdownHookManager.class);



  private static final ExecutorService EXECUTOR =
      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("shutdown-hook-%01d")
          .build());

  static {
    try {
      Runtime.getRuntime().addShutdownHook(
          new Thread() {
            @Override
            public void run() {
              if (MGR.shutdownInProgress.getAndSet(true)) {
                LOG.info("Shutdown process invoked a second time: ignoring");
                return;
              }
              long started = System.currentTimeMillis();
              int timeoutCount = MGR.executeShutdown();
              long ended = System.currentTimeMillis();
              LOG.debug(String.format(
                  "Completed shutdown in %.3f seconds; Timeouts: %d",
                  (ended - started) / 1000.0, timeoutCount));
              // each of the hooks have executed; now shut down the
              // executor itself.
              shutdownExecutor(new OzoneConfiguration());
            }
          }
      );
    } catch (IllegalStateException ex) {
      // JVM is being shut down. Ignore
      LOG.warn("Failed to add the ShutdownHook", ex);
    }
  }

  /**
   * Execute the shutdown.
   * This is exposed purely for testing: do not invoke it.
   * @return the number of shutdown hooks which timed out.
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  int executeShutdown() {
    int timeouts = 0;
    for (HookEntry entry: getShutdownHooksInOrder()) {
      Future<?> future = EXECUTOR.submit(entry.getHook());
      try {
        future.get(entry.getTimeout(), entry.getTimeUnit());
      } catch (TimeoutException ex) {
        timeouts++;
        future.cancel(true);
        LOG.warn("ShutdownHook '" + entry.getHook().getClass().
            getSimpleName() + "' timeout, " + ex.toString(), ex);
      } catch (InterruptedException ex) {
        LOG.warn("ShutdownHook '" + entry.getHook().getClass().
            getSimpleName() + "' failed, " + ex.toString(), ex);
        Thread.currentThread().interrupt();
      } catch (ExecutionException ex) {
        LOG.warn("ShutdownHook '" + entry.getHook().getClass().
            getSimpleName() + "' failed, " + ex.toString(), ex);
      }
    }
    return timeouts;
  }

  /**
   * Shutdown the executor thread itself.
   * @param conf the configuration containing the shutdown timeout setting.
   */
  private static void shutdownExecutor(final ConfigurationSource conf) {
    try {
      EXECUTOR.shutdown();
      long shutdownTimeout = getShutdownTimeout(conf);
      if (!EXECUTOR.awaitTermination(
          shutdownTimeout, OZONE_SHUTDOWN_TIME_UNIT_DEFAULT)) {
        // timeout waiting for the
        LOG.error("ShutdownHookManger shutdown forcefully after"
            + " {} seconds.", shutdownTimeout);
        EXECUTOR.shutdownNow();
      }
      LOG.debug("ShutdownHookManger completed shutdown.");
    } catch (InterruptedException ex) {
      // interrupted.
      LOG.error("ShutdownHookManger interrupted while waiting for " +
          "termination.", ex);
      EXECUTOR.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Return <code>ShutdownHookManager</code> singleton.
   *
   * @return <code>ShutdownHookManager</code> singleton.
   */
  @InterfaceAudience.Public
  public static ShutdownHookManager get() {
    return MGR;
  }

  /**
   * Get the shutdown timeout in seconds, from the supplied
   * configuration.
   * @param conf configuration to use.
   * @return a timeout, always greater than or equal to
   * {@link org.apache.hadoop.ozone.conf.OzoneServiceConfig
   * #OZONE_SHUTDOWN_TIMEOUT_MINIMUM}
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  static long getShutdownTimeout(ConfigurationSource conf) {
    long duration = HddsUtils.getShutDownTimeOut(conf);
    if (duration < OZONE_SHUTDOWN_TIMEOUT_MINIMUM) {
      duration = OZONE_SHUTDOWN_TIMEOUT_MINIMUM;
    }
    return duration;
  }

  /**
   * Private structure to store ShutdownHook, its priority and timeout
   * settings.
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  static class HookEntry {
    private final Runnable hook;
    private final int priority;
    private final long timeout;
    private final TimeUnit unit;

    HookEntry(Runnable hook, int priority) {
      this(hook, priority, getShutdownTimeout(new OzoneConfiguration()),
              OZONE_SHUTDOWN_TIME_UNIT_DEFAULT);
    }

    HookEntry(Runnable hook, int priority, long timeout, TimeUnit unit) {
      this.hook = hook;
      this.priority = priority;
      this.timeout = timeout;
      this.unit = unit;
    }

    @Override
    public int hashCode() {
      return hook.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      boolean eq = false;
      if (obj != null) {
        if (obj instanceof HookEntry) {
          eq = (hook == ((HookEntry)obj).hook);
        }
      }
      return eq;
    }

    Runnable getHook() {
      return hook;
    }

    int getPriority() {
      return priority;
    }

    long getTimeout() {
      return timeout;
    }

    TimeUnit getTimeUnit() {
      return unit;
    }
  }

  private final Set<HookEntry> hooks =
      Collections.synchronizedSet(new HashSet<>());

  private AtomicBoolean shutdownInProgress = new AtomicBoolean(false);

  //private to constructor to ensure singularity
  @VisibleForTesting
  @InterfaceAudience.Private
  ShutdownHookManager() {
  }

  /**
   * Returns the list of shutdownHooks in order of execution,
   * Highest priority first.
   *
   * @return the list of shutdownHooks in order of execution.
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  List<HookEntry > getShutdownHooksInOrder() {
    List<HookEntry > list;
    synchronized (hooks) {
      list = new ArrayList<HookEntry>(hooks);
    }
    Collections.sort(list, new Comparator< HookEntry >() {

      //reversing comparison so highest priority hooks are first
      @Override
      public int compare(HookEntry o1, HookEntry o2) {
        return o2.priority - o1.priority;
      }
    });
    return list;
  }

  /**
   * Adds a shutdownHook with a priority, the higher the priority
   * the earlier will run. ShutdownHooks with same priority run
   * in a non-deterministic order.
   *
   * @param shutdownHook shutdownHook <code>Runnable</code>
   * @param priority priority of the shutdownHook.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public void addShutdownHook(Runnable shutdownHook, int priority) {
    if (shutdownHook == null) {
      throw new IllegalArgumentException("shutdownHook cannot be NULL");
    }
    if (shutdownInProgress.get()) {
      throw new IllegalStateException("Shutdown in progress, cannot add a " +
          "shutdownHook");
    }
    hooks.add(new HookEntry(shutdownHook, priority));
  }

  /**
   *
   * Adds a shutdownHook with a priority and timeout the higher the priority
   * the earlier will run. ShutdownHooks with same priority run
   * in a non-deterministic order. The shutdown hook will be terminated if it
   * has not been finished in the specified period of time.
   *
   * @param shutdownHook shutdownHook <code>Runnable</code>
   * @param priority priority of the shutdownHook
   * @param timeout timeout of the shutdownHook
   * @param unit unit of the timeout <code>TimeUnit</code>
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public void addShutdownHook(Runnable shutdownHook, int priority, long timeout,
      TimeUnit unit) {
    if (shutdownHook == null) {
      throw new IllegalArgumentException("shutdownHook cannot be NULL");
    }
    if (shutdownInProgress.get()) {
      throw new IllegalStateException("Shutdown in progress, cannot add a " +
          "shutdownHook");
    }
    hooks.add(new HookEntry(shutdownHook, priority, timeout, unit));
  }

  /**
   * Removes a shutdownHook.
   *
   * @param shutdownHook shutdownHook to remove.
   * @return TRUE if the shutdownHook was registered and removed,
   * FALSE otherwise.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public boolean removeShutdownHook(Runnable shutdownHook) {
    if (shutdownInProgress.get()) {
      throw new IllegalStateException("Shutdown in progress, cannot remove a " +
          "shutdownHook");
    }
    // hooks are only == by runnable
    return hooks.remove(new HookEntry(shutdownHook, 0,
            OZONE_SHUTDOWN_TIMEOUT_MINIMUM,
            OZONE_SHUTDOWN_TIME_UNIT_DEFAULT));
  }

  /**
   * Indicates if a shutdownHook is registered or not.
   *
   * @param shutdownHook shutdownHook to check if registered.
   * @return TRUE/FALSE depending if the shutdownHook is is registered.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public boolean hasShutdownHook(Runnable shutdownHook) {
    return hooks.contains(new HookEntry(shutdownHook, 0,
            OZONE_SHUTDOWN_TIMEOUT_MINIMUM,
            OZONE_SHUTDOWN_TIME_UNIT_DEFAULT));
  }

  /**
   * Indicates if shutdown is in progress or not.
   *
   * @return TRUE if the shutdown is in progress, otherwise FALSE.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public boolean isShutdownInProgress() {
    return shutdownInProgress.get();
  }

  /**
   * clear all registered shutdownHooks.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public void clearShutdownHooks() {
    hooks.clear();
  }
}


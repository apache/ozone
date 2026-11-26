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

package org.apache.hadoop.ozone.om.lock;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_HARD_LIMIT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_HARD_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_SOFT_LIMIT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_SOFT_LIMIT_DEFAULT;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lock manager implementation that manages hierarchical resource locks
 * using a pool of reusable {@link ReadWriteLock} instances. The implementation
 * ensures deterministic lock ordering for resources, avoiding cyclic
 * lock dependencies, and is typically useful for structures like
 * DAGs (e.g., File System trees or snapshot chains).
 */
public class PoolBasedHierarchicalResourceLockManager implements HierarchicalResourceLockManager {

  private static final Logger LOG = LoggerFactory.getLogger(PoolBasedHierarchicalResourceLockManager.class);

  private final GenericObjectPool<ReadWriteLock> lockPool;
  private final ResourceLockTracker<DAGLeveledResource> resourceLockTracker;
  private final Map<DAGLeveledResource, Pair<ReadWriteLock, Map<String, LockReferenceCountPair>>> lockMap;

  public PoolBasedHierarchicalResourceLockManager(OzoneConfiguration conf) {
    int softLimit = conf.getInt(OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_SOFT_LIMIT,
        OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_SOFT_LIMIT_DEFAULT);
    int hardLimit = conf.getInt(OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_HARD_LIMIT,
        OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_HARD_LIMIT_DEFAULT);
    GenericObjectPoolConfig<ReadWriteLock> config = new GenericObjectPoolConfig<>();
    config.setMaxIdle(softLimit);
    config.setMaxTotal(hardLimit);
    config.setBlockWhenExhausted(true);
    this.lockPool = new GenericObjectPool<>(new ReadWriteLockFactory(), config);
    this.lockMap = new EnumMap<>(DAGLeveledResource.class);
    for (DAGLeveledResource resource : DAGLeveledResource.values()) {
      this.lockMap.put(resource, Pair.of(new ReentrantReadWriteLock(), new ConcurrentHashMap<>(0)));
    }
    this.resourceLockTracker = DAGResourceLockTracker.get();
  }

  private ReadWriteLock operateOnLock(DAGLeveledResource resource, String key,
      Consumer<LockReferenceCountPair> function) throws IOException {
    AtomicReference<IOException> exception = new AtomicReference<>();
    Pair<ReadWriteLock, Map<String, LockReferenceCountPair>> resourceLockPair = this.lockMap.get(resource);
    Map<String, LockReferenceCountPair> resourceLockMap = resourceLockPair.getValue();
    LockReferenceCountPair lockRef = resourceLockMap.compute(key, (k, v) -> {
      if (v == null) {
        try {
          ReadWriteLock readWriteLock = this.lockPool.borrowObject();
          v = new LockReferenceCountPair(readWriteLock);
        } catch (Exception e) {
          exception.set(new IOException("Exception while initializing lock object.", e));
          return null;
        }
      }
      function.accept(v);
      Preconditions.checkState(v.getCount() >= 0);
      if (v.getCount() == 0) {
        this.lockPool.returnObject(v.getLock());
        return null;
      }
      return v;
    });
    if (exception.get() != null) {
      throw exception.get();
    }
    return lockRef == null ? null : lockRef.getLock();
  }

  @Override
  public HierarchicalResourceLock acquireReadLock(DAGLeveledResource resource, String key) throws IOException {
    return acquireLock(resource, key, true);
  }

  @Override
  public HierarchicalResourceLock acquireWriteLock(DAGLeveledResource resource, String key) throws IOException {
    return acquireLock(resource, key, false);
  }

  @Override
  public HierarchicalResourceLock acquireResourceWriteLock(DAGLeveledResource resource) throws IOException {
    if (!resourceLockTracker.canLockResource(resource)) {
      String errorMessage = getErrorMessage(resource);
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    }
    return new PoolBasedHierarchicalResourceLock(resource);
  }

  private String getErrorMessage(IOzoneManagerLock.Resource resource) {
    return "Thread '" + Thread.currentThread().getName() + "' cannot " +
        "acquire " + resource.getName() + " lock while holding " +
        resourceLockTracker.getCurrentLockedResources().map(IOzoneManagerLock.Resource::getName)
            .collect(Collectors.toList()) + " lock(s).";
  }

  @Override
  public Stream<DAGLeveledResource> getCurrentLockedResources() {
    return resourceLockTracker.getCurrentLockedResources();
  }

  private HierarchicalResourceLock acquireLock(DAGLeveledResource resource, String key, boolean isReadLock)
      throws IOException {
    if (!resourceLockTracker.canLockResource(resource)) {
      String errorMessage = getErrorMessage(resource);
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    }
    ReadWriteLock readWriteLock = operateOnLock(resource, key, LockReferenceCountPair::increment);
    if (readWriteLock == null) {
      throw new IOException("Unable to acquire " + (isReadLock ? "read" : "write") + " lock on resource "
          + resource + " and key " + key);
    }
    return new PoolBasedHierarchicalResourceKeyLock(resource, key,
        isReadLock ? readWriteLock.readLock() : readWriteLock.writeLock());
  }

  @Override
  public void close() {
    this.lockPool.close();
  }

  /**
   * The PoolBasedHierachicalResourceLock class implements the HierarchicalResourceLock
   * and UncheckedAutoCloseable interfaces to manage hierarchical resource locks from
   * a shared pool. It ensures proper lock acquisition and release during its lifecycle.
   */
  private final class PoolBasedHierarchicalResourceLock implements HierarchicalResourceLock,
      UncheckedAutoCloseable {
    private final DAGLeveledResource resource;
    private final Lock resourceLock;
    private boolean lockAcquired;

    private PoolBasedHierarchicalResourceLock(DAGLeveledResource resource) {
      this.resource = resource;
      this.resourceLock = lockMap.get(this.resource).getKey().writeLock();
      resourceLock.lock();
      resourceLockTracker.lockResource(this.resource);
      lockAcquired = true;
    }

    @Override
    public boolean isLockAcquired() {
      return lockAcquired;
    }

    @Override
    public synchronized void close() {
      if (lockAcquired) {
        resourceLock.unlock();
        resourceLockTracker.unlockResource(this.resource);
        lockAcquired = false;
      }
    }
  }

  /**
   * Represents a hierarchical resource lock mechanism that operates
   * using a resource pool for acquiring and releasing locks. This class
   * provides thread-safe management of read and write locks associated
   * with specific hierarchical resources.
   *
   * A lock can either be a read lock or a write lock. This is determined
   * at the time of instantiation. The lifecycle of the lock is managed
   * through this class, and the lock is automatically released when the
   * `close` method is invoked.
   *
   * This is designed to work in conjunction with the containing manager
   * class, {@code PoolBasedHierarchicalResourceLockManager}, which oversees
   * the lifecycle of multiple such locks.
   */
  private final class PoolBasedHierarchicalResourceKeyLock implements HierarchicalResourceLock, Closeable {

    private boolean isLockAcquired;
    private final Lock resourceLock;
    private final Lock keyLock;
    private final DAGLeveledResource resource;
    private final String key;

    private PoolBasedHierarchicalResourceKeyLock(DAGLeveledResource resource, String key, Lock lock) {
      this.keyLock = lock;
      this.resource = resource;
      this.key = key;
      this.resourceLock = lockMap.get(resource).getKey().readLock();
      this.resourceLock.lock();
      this.keyLock.lock();
      resourceLockTracker.lockResource(resource);
      this.isLockAcquired = true;
    }

    @Override
    public boolean isLockAcquired() {
      return isLockAcquired;
    }

    @Override
    public synchronized void close() throws IOException {
      if (isLockAcquired) {
        this.keyLock.unlock();
        this.resourceLock.unlock();
        resourceLockTracker.unlockResource(resource);
        operateOnLock(resource, key, (LockReferenceCountPair::decrement));
        isLockAcquired = false;
      }
    }
  }

  private static final class LockReferenceCountPair {
    private int count;
    private final ReadWriteLock lock;

    private LockReferenceCountPair(ReadWriteLock lock) {
      this.count = 0;
      this.lock = lock;
    }

    private void increment() {
      count++;
    }

    private void decrement() {
      count--;
    }

    private int getCount() {
      return count;
    }

    private ReadWriteLock getLock() {
      return lock;
    }
  }

  private static class ReadWriteLockFactory extends BasePooledObjectFactory<ReadWriteLock>  {

    @Override
    public ReadWriteLock create() {
      return new ReentrantReadWriteLock();
    }

    @Override
    public PooledObject<ReadWriteLock> wrap(ReadWriteLock obj) {
      return new DefaultPooledObject<>(obj);
    }
  }
}

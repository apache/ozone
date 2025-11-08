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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * A lock manager implementation that manages hierarchical resource locks
 * using a pool of reusable {@link ReadWriteLock} instances. The implementation
 * ensures deterministic lock ordering for resources, avoiding cyclic
 * lock dependencies, and is typically useful for structures like
 * DAGs (e.g., File System trees or snapshot chains).
 */
public class PoolBasedHierarchicalResourceLockManager implements HierarchicalResourceLockManager {
  private final GenericObjectPool<ReadWriteLock> lockPool;
  private final Map<FlatResource, Map<String, LockReferenceCountPair>> lockMap;

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
    this.lockMap = new ConcurrentHashMap<>();
  }

  private ReadWriteLock operateOnLock(FlatResource resource, String key, Consumer<LockReferenceCountPair> function)
      throws IOException {
    AtomicReference<IOException> exception = new AtomicReference<>();
    Map<String, LockReferenceCountPair> resourceLockMap =
        this.lockMap.computeIfAbsent(resource, k -> new ConcurrentHashMap<>());
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
  public HierarchicalResourceLock acquireReadLock(FlatResource resource, String key) throws IOException {
    return acquireLock(resource, key, true);
  }

  @Override
  public HierarchicalResourceLock acquireWriteLock(FlatResource resource, String key) throws IOException {
    return acquireLock(resource, key, false);
  }

  private HierarchicalResourceLock acquireLock(FlatResource resource, String key, boolean isReadLock)
      throws IOException {
    ReadWriteLock readWriteLock = operateOnLock(resource, key, LockReferenceCountPair::increment);
    if (readWriteLock == null) {
      throw new IOException("Unable to acquire " + (isReadLock ? "read" : "write") + " lock on resource "
          + resource + " and key " + key);
    }
    return new PoolBasedHierarchicalResourceLock(resource, key,
        isReadLock ? readWriteLock.readLock() : readWriteLock.writeLock());
  }

  @Override
  public void close() {
    this.lockPool.close();
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
  public class PoolBasedHierarchicalResourceLock implements HierarchicalResourceLock, Closeable {

    private boolean isLockAcquired;
    private final Lock lock;
    private final FlatResource resource;
    private final String key;

    public PoolBasedHierarchicalResourceLock(FlatResource resource, String key, Lock lock) {
      this.isLockAcquired = true;
      this.lock = lock;
      this.resource = resource;
      this.key = key;
      this.lock.lock();
    }

    @Override
    public boolean isLockAcquired() {
      return isLockAcquired;
    }

    @Override
    public synchronized void close() throws IOException {
      if (isLockAcquired) {
        this.lock.unlock();
        operateOnLock(resource, key, (LockReferenceCountPair::decrement));
        isLockAcquired = false;
      }
    }
  }

  private static final class LockReferenceCountPair {
    private int count;
    private ReadWriteLock lock;

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
    public ReadWriteLock create() throws Exception {
      return new ReentrantReadWriteLock();
    }

    @Override
    public PooledObject<ReadWriteLock> wrap(ReadWriteLock obj) {
      return new DefaultPooledObject<>(obj);
    }
  }
}

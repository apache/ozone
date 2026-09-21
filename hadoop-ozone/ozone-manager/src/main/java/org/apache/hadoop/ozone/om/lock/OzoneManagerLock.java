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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_FAIR_LOCK;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_FAIR_LOCK_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_STRIPED_LOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_STRIPED_LOCK_SIZE_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Striped;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.CompositeKey;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.apache.hadoop.ipc_.ProcessingDetails.Timing;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides different locks to handle concurrency in OzoneMaster.
 * We also maintain lock hierarchy, based on the weight.
 *
 * <table>
 *   <caption></caption>
 *   <tr>
 *     <td><b> WEIGHT </b></td> <td><b> LOCK </b></td>
 *   </tr>
 *   <tr>
 *     <td> 0 </td> <td> S3 Bucket Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 1 </td> <td> Volume Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 2 </td> <td> Bucket Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 3 </td> <td> User Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 4 </td> <td> S3 Secret Lock</td>
 *   </tr>
 *   <tr>
 *     <td> 5 </td> <td> Prefix Lock </td>
 *   </tr>
 * </table>
 *
 * One cannot obtain a lower weight lock while holding a lock with higher
 * weight. The other way around is possible. <br>
 * <br>
 * <p>
 * For example:
 * <br>
 * {@literal ->} acquire volume lock (will work)<br>
 *   {@literal +->} acquire bucket lock (will work)<br>
 *     {@literal +-->} acquire s3 bucket lock (will throw Exception)<br>
 * </p>
 * <br>
 */

public class OzoneManagerLock implements IOzoneManagerLock {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerLock.class);

  private final ResourceLocks<LeveledResource> leveledResourceLocks;
  private final ResourceLocks<DAGLeveledResource> dagLeveledResourceLocks;

  private final OMLockMetrics omLockMetrics = OMLockMetrics.create();

  class ResourceLocks<R extends Resource> {
    private final Map<R, Striped<ReentrantReadWriteLock>> lockMap;
    private final ResourceLockTracker<R> tracker;

    ResourceLocks(Map<R, Striped<ReentrantReadWriteLock>> lockMap, ResourceLockTracker<R> tracker) {
      this.lockMap = lockMap;
      this.tracker = tracker;
    }

    R assertAcquire(Resource resource) {
      final R r = Preconditions.assertInstanceOf(resource, tracker.getResourceClass());
      tracker.clearLockDetails();
      if (!tracker.canLockResource(r)) {
        final String errorMessage =  "Thread '" + Thread.currentThread().getName() + "' cannot acquire "
            + r.getName() + " lock while holding " + getCurrentLocks() + " lock(s).";
        LOG.error(errorMessage);
        // TODO: change it to IllegalStateException
        throw new RuntimeException(errorMessage);
      }
      return r;
    }

    private ReentrantReadWriteLock getLockForTesting(Resource resource, String... keys) {
      final R r = Preconditions.assertInstanceOf(resource, tracker.getResourceClass());
      return getLockWithCombinedKey(r, CompositeKey.combineKeys(keys));
    }

    private ReentrantReadWriteLock getLockWithCombinedKey(R r, Object combinedKey) {
      return lockMap.get(r).get(combinedKey);
    }

    private void acquireLock(R resource, boolean isRead, ReentrantReadWriteLock lock, long startWaitingTimeNanos) {
      if (isRead) {
        lock.readLock().lock();
        updateReadLockMetrics(resource, tracker, lock, startWaitingTimeNanos);
      } else {
        lock.writeLock().lock();
        updateWriteLockMetrics(resource, tracker, lock, startWaitingTimeNanos);
      }
    }

    private OMLockDetails acquireImpl(Resource resource, BiConsumer<R, Long> acquireLockMethod) {
      final R r = assertAcquire(resource);
      final long startWaitingTimeNanos = Time.monotonicNowNanos();
      acquireLockMethod.accept(r, startWaitingTimeNanos);
      return tracker.lockResource(r);
    }

    private OMLockDetails acquireOne(Resource resource, boolean isRead, Object combinedKey) {
      return acquireImpl(resource, (r, startWaitingTimeNanos) -> {
        final ReentrantReadWriteLock lock = getLockWithCombinedKey(r, combinedKey);
        acquireLock(r, isRead, lock, startWaitingTimeNanos);
      });
    }

    private OMLockDetails acquireAll(Resource resource) {
      return acquireImpl(resource, (r, startWaitingTimeNanos) -> {
        final Striped<ReentrantReadWriteLock> striped = lockMap.get(r);
        for (int i = 0; i < striped.size(); i++) {
          acquireLock(r, false, striped.getAt(i), startWaitingTimeNanos);
        }
      });
    }

    private OMLockDetails acquireSelected(Resource resource, boolean isRead, Iterable<String[]> keys) {
      return acquireImpl(resource, (r, startWaitingTimeNanos) -> {
        for (ReentrantReadWriteLock lock : bulkGetForAcquire(lockMap.get(r), keys)) {
          acquireLock(r, isRead, lock, startWaitingTimeNanos);
        }
      });
    }

    private void releaseLock(R resource, boolean isRead, ReentrantReadWriteLock lock) {
      if (isRead) {
        lock.readLock().unlock();
        updateReadUnlockMetrics(resource, tracker, lock);
      } else {
        boolean isWriteLocked = lock.isWriteLockedByCurrentThread();
        lock.writeLock().unlock();
        updateWriteUnlockMetrics(resource, tracker, lock, isWriteLocked);
      }
    }

    private OMLockDetails releaseImpl(Resource resource, Consumer<R> releaseLockMethod) {
      final R r = Preconditions.assertInstanceOf(resource, tracker.getResourceClass());
      tracker.clearLockDetails();
      releaseLockMethod.accept(r);
      return tracker.unlockResource(r);
    }

    private OMLockDetails releaseOne(Resource resource, boolean isRead, Object combinedKey) {
      return releaseImpl(resource, r -> {
        final ReentrantReadWriteLock lock = getLockWithCombinedKey(r, combinedKey);
        releaseLock(r, isRead, lock);
      });
    }

    private OMLockDetails releaseAll(Resource resource) {
      return releaseImpl(resource, r -> {
        final Striped<ReentrantReadWriteLock> striped = lockMap.get(r);
        // Release locks in reverse order.
        for (int i = striped.size() - 1; i >= 0; i--) {
          releaseLock(r, false, striped.getAt(i));
        }
      });
    }

    private OMLockDetails releaseSelected(Resource resource, boolean isRead, Iterable<String[]> keys) {
      return releaseImpl(resource, r -> {
        for (ReentrantReadWriteLock lock : bulkGetForRelease(lockMap.get(r), keys)) {
          releaseLock(r, isRead, lock);
        }
      });
    }

    List<String> getCurrentLocks() {
      return tracker.getCurrentLockedResources()
          .map(Resource::getName)
          .collect(Collectors.toList());
    }
  }

  /**
   * Creates new OzoneManagerLock instance.
   * @param conf Configuration object
   */
  public OzoneManagerLock(ConfigurationSource conf) {
    this.leveledResourceLocks = newResourceLocks(LeveledResourceLockTracker.get(), conf);
    this.dagLeveledResourceLocks = newResourceLocks(DAGResourceLockTracker.get(), conf);
  }

  private <T extends Enum<T> & Resource> ResourceLocks<T> newResourceLocks(
      ResourceLockTracker<T> tracker, ConfigurationSource conf) {
    final Class<T> clazz = tracker.getResourceClass();
    final EnumMap<T, Striped<ReentrantReadWriteLock>> stripedLockMap = new EnumMap<>(clazz);
    for (T r : clazz.getEnumConstants()) {
      stripedLockMap.put(r, createStripeLock(r, conf));
    }
    return new ResourceLocks<>(Collections.unmodifiableMap(stripedLockMap), tracker);
  }

  private ResourceLocks<?> getResourceLocks(Resource instance) {
    final Class<?> clazz = instance.getClass();
    if (clazz == LeveledResource.class) {
      return leveledResourceLocks;
    } else if (clazz == DAGLeveledResource.class) {
      return dagLeveledResourceLocks;
    }
    throw new IllegalArgumentException("Unsupported resource class: " + clazz);
  }

  private static Striped<ReentrantReadWriteLock> createStripeLock(Resource r, ConfigurationSource conf) {
    boolean fair = conf.getBoolean(OZONE_MANAGER_FAIR_LOCK,
        OZONE_MANAGER_FAIR_LOCK_DEFAULT);
    String stripeSizeKey = OZONE_MANAGER_STRIPED_LOCK_SIZE_PREFIX +
        r.getName().toLowerCase();
    int size = conf.getInt(stripeSizeKey,
        OZONE_MANAGER_STRIPED_LOCK_SIZE_DEFAULT);
    return SimpleStriped.readWriteLock(size, fair);
  }

  /** @return locks in ascending order for acquire. */
  static Iterable<ReentrantReadWriteLock> bulkGetForAcquire(
      Striped<ReentrantReadWriteLock> striped, Iterable<String[]> keys) {
    return striped.bulkGet(CollectionUtils.as(keys, CompositeKey::combineKeys)); // no copying
  }

  /** @return locks in descending order for release. */
  static Iterable<ReentrantReadWriteLock> bulkGetForRelease(
      Striped<ReentrantReadWriteLock> striped, Iterable<String[]> keys) {
    final Iterable<ReentrantReadWriteLock> iterable = bulkGetForAcquire(striped, keys);

    // although the return type of Striped.bulkGet(..) is Iterable, its implementation currently returns an ArrayList.
    if (iterable instanceof List && iterable instanceof RandomAccess) {
      final List<ReentrantReadWriteLock> list = (List<ReentrantReadWriteLock>) iterable;
      // return in descending order
      return () -> new Iterator<ReentrantReadWriteLock>() {
        private int i = list.size() - 1;

        @Override
        public boolean hasNext() {
          return i >= 0;
        }

        @Override
        public ReentrantReadWriteLock next() {
          return list.get(i--);
        }
      };
    }

    // use Deque
    final Deque<ReentrantReadWriteLock> deque;
    if (iterable instanceof Deque) {
      deque = (Deque<ReentrantReadWriteLock>) iterable;
    } else {
      // fallback copying to a list
      deque = new LinkedList<>();
      for (ReentrantReadWriteLock lock : iterable) {
        deque.add(lock);
      }
    }
    return deque::descendingIterator;
  }

  @Override
  public OMLockDetails acquireReadLock(Resource resource, String key) {
    return getResourceLocks(resource)
        .acquireOne(resource, true, key);
  }

  @Override
  public OMLockDetails acquireReadLock(Resource resource, String key1, String key2) {
    return getResourceLocks(resource)
        .acquireOne(resource, true, CompositeKey.combineTwoKeys(key1, key2));
  }

  @Override
  public OMLockDetails acquireReadLock(Resource resource, String... keys) {
    Preconditions.assertTrue(keys.length > 2);
    return getResourceLocks(resource)
        .acquireOne(resource, true, CompositeKey.combineMultiKeys(keys));
  }

  @Override
  public OMLockDetails acquireReadLocks(Resource resource, Iterable<String[]> keys) {
    return getResourceLocks(resource)
        .acquireSelected(resource, true, keys);
  }

  @Override
  public OMLockDetails acquireWriteLock(Resource resource, String key) {
    return getResourceLocks(resource)
        .acquireOne(resource, false, key);
  }

  @Override
  public OMLockDetails acquireWriteLock(Resource resource, String key1, String key2) {
    return getResourceLocks(resource)
        .acquireOne(resource, false, CompositeKey.combineTwoKeys(key1, key2));
  }

  @Override
  public OMLockDetails acquireWriteLock(Resource resource, String... keys) {
    return getResourceLocks(resource)
        .acquireOne(resource, false, CompositeKey.combineMultiKeys(keys));
  }

  /**
   * Acquire write locks on a list of resources.
   *
   * For S3_BUCKET_LOCK, VOLUME_LOCK, BUCKET_LOCK type resource, same
   * thread acquiring lock again is allowed.
   *
   * For USER_LOCK, PREFIX_LOCK, S3_SECRET_LOCK type resource, same thread
   * acquiring lock again is not allowed.
   *
   * Special Note for USER_LOCK: Single thread can acquire single user lock/
   * multi user lock. But not both at the same time.
   * @param resource - Type of the resource.
   * @param keys - A list of Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  @Override
  public OMLockDetails acquireWriteLocks(Resource resource, Iterable<String[]> keys) {
    return getResourceLocks(resource)
        .acquireSelected(resource, false, keys);
  }

  /**
   * Acquires all write locks for a specified resource.
   *
   * @param resource The resource for which the write lock is to be acquired.
   */
  @Override
  public OMLockDetails acquireResourceWriteLock(Resource resource) {
    return getResourceLocks(resource)
        .acquireAll(resource);
  }

  private void updateReadLockMetrics(Resource resource, ResourceLockTracker<? extends Resource> tracker,
      ReentrantReadWriteLock lock, long startWaitingTimeNanos) {

    /*
     *  readHoldCount helps in metrics updation only once in case
     *  of reentrant locks.
     */
    if (lock.getReadHoldCount() == 1) {
      long readLockWaitingTimeNanos =
          Time.monotonicNowNanos() - startWaitingTimeNanos;

      // Adds a snapshot to the metric readLockWaitingTimeMsStat.
      omLockMetrics.setReadLockWaitingTimeMsStat(
          TimeUnit.NANOSECONDS.toMillis(readLockWaitingTimeNanos));
      updateProcessingDetails(tracker, Timing.LOCKWAIT, readLockWaitingTimeNanos);

      resource.getResourceManager().setStartReadHeldTimeNanos(Time.monotonicNowNanos());
    }
  }

  private void updateWriteLockMetrics(Resource resource, ResourceLockTracker<? extends Resource> tracker,
      ReentrantReadWriteLock lock, long startWaitingTimeNanos) {
    /*
     *  writeHoldCount helps in metrics updation only once in case
     *  of reentrant locks. Metrics are updated only if the write lock is held
     *  by the current thread.
     */
    if ((lock.getWriteHoldCount() == 1) &&
        lock.isWriteLockedByCurrentThread()) {
      long writeLockWaitingTimeNanos =
          Time.monotonicNowNanos() - startWaitingTimeNanos;

      // Adds a snapshot to the metric writeLockWaitingTimeMsStat.
      omLockMetrics.setWriteLockWaitingTimeMsStat(
          TimeUnit.NANOSECONDS.toMillis(writeLockWaitingTimeNanos));
      updateProcessingDetails(tracker, Timing.LOCKWAIT, writeLockWaitingTimeNanos);

      resource.getResourceManager().setStartWriteHeldTimeNanos(Time.monotonicNowNanos());
    }
  }

  @VisibleForTesting
  int getCurrentLockSizeForTesting() {
    return leveledResourceLocks.getCurrentLocks().size() + dagLeveledResourceLocks.getCurrentLocks().size();
  }

  /**
   * Acquire lock on multiple users.
   */
  @Override
  public boolean acquireMultiUserLock(String firstUser, String secondUser) {
    return acquireWriteLocks(LeveledResource.USER_LOCK,
        Arrays.asList(new String[] {firstUser}, new String[] {secondUser})).isLockAcquired();
  }

  /**
   * Release lock on multiple users.
   * @param firstUser
   * @param secondUser
   */
  @Override
  public void releaseMultiUserLock(String firstUser, String secondUser) {
    releaseWriteLocks(LeveledResource.USER_LOCK,
        Arrays.asList(new String[] {firstUser}, new String[] {secondUser}));
  }

  @Override
  public OMLockDetails releaseWriteLock(Resource resource, String key) {
    return getResourceLocks(resource)
        .releaseOne(resource, false, key);
  }

  @Override
  public OMLockDetails releaseWriteLock(Resource resource, String key1, String key2) {
    return getResourceLocks(resource)
        .releaseOne(resource, false, CompositeKey.combineTwoKeys(key1, key2));
  }

  @Override
  public OMLockDetails releaseWriteLock(Resource resource, String... keys) {
    return getResourceLocks(resource)
        .releaseOne(resource, false, CompositeKey.combineMultiKeys(keys));
  }

  /**
   * Release write lock on multiple resources.
   * @param resource - Type of the resource.
   * @param keys - List of resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  @Override
  public OMLockDetails releaseWriteLocks(Resource resource, Iterable<String[]> keys) {
    return getResourceLocks(resource)
        .releaseSelected(resource, false, keys);
  }

  /**
   * Releases a write lock acquired on the entire Stripe for a specified resource.
   *
   * @param resource The resource for which the write lock is to be acquired.
   */
  @Override
  public OMLockDetails releaseResourceWriteLock(Resource resource) {
    return getResourceLocks(resource)
        .releaseAll(resource);
  }

  @Override
  public OMLockDetails releaseReadLock(Resource resource, String key) {
    return getResourceLocks(resource)
        .releaseOne(resource, true, key);
  }

  @Override
  public OMLockDetails releaseReadLock(Resource resource, String key1, String key2) {
    return getResourceLocks(resource)
        .releaseOne(resource, true, CompositeKey.combineTwoKeys(key1, key2));
  }

  @Override
  public OMLockDetails releaseReadLock(Resource resource, String... keys) {
    return getResourceLocks(resource)
        .releaseOne(resource, true, CompositeKey.combineMultiKeys(keys));
  }

  /**
   * Release read locks on a list of resources.
   * @param resource - Type of the resource.
   * @param keys - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  @Override
  public OMLockDetails releaseReadLocks(Resource resource, Iterable<String[]> keys) {
    return getResourceLocks(resource)
        .releaseSelected(resource, true, keys);
  }

  private void updateReadUnlockMetrics(Resource resource, ResourceLockTracker<? extends Resource> tracker,
      ReentrantReadWriteLock lock) {
    /*
     *  readHoldCount helps in metrics updation only once in case
     *  of reentrant locks.
     */
    if (lock.getReadHoldCount() == 0) {
      long readLockHeldTimeNanos =
          Time.monotonicNowNanos() - resource.getResourceManager().getStartReadHeldTimeNanos();

      // Adds a snapshot to the metric readLockHeldTimeMsStat.
      omLockMetrics.setReadLockHeldTimeMsStat(
          TimeUnit.NANOSECONDS.toMillis(readLockHeldTimeNanos));
      updateProcessingDetails(tracker, Timing.LOCKSHARED, readLockHeldTimeNanos);
    }
  }

  private void updateWriteUnlockMetrics(Resource resource, ResourceLockTracker<? extends Resource> tracker,
      ReentrantReadWriteLock lock, boolean isWriteLocked) {
    /*
     *  writeHoldCount helps in metrics updation only once in case
     *  of reentrant locks. Metrics are updated only if the write lock is held
     *  by the current thread.
     */
    if ((lock.getWriteHoldCount() == 0) && isWriteLocked) {
      long writeLockHeldTimeNanos =
          Time.monotonicNowNanos() - resource.getResourceManager().getStartWriteHeldTimeNanos();

      // Adds a snapshot to the metric writeLockHeldTimeMsStat.
      omLockMetrics.setWriteLockHeldTimeMsStat(
          TimeUnit.NANOSECONDS.toMillis(writeLockHeldTimeNanos));
      updateProcessingDetails(tracker, Timing.LOCKEXCLUSIVE, writeLockHeldTimeNanos);
    }
  }

  /**
   * Returns readHoldCount for a given resource lock name.
   *
   * @return readHoldCount
   */
  @Override
  @VisibleForTesting
  public int getReadHoldCount(Resource resource, String... keys) {
    return getResourceLocks(resource).getLockForTesting(resource, keys).getReadHoldCount();
  }


  /**
   * Returns writeHoldCount for a given resource lock name.
   *
   * @return writeHoldCount
   */
  @Override
  @VisibleForTesting
  public int getWriteHoldCount(Resource resource, String... keys) {
    return getResourceLocks(resource).getLockForTesting(resource, keys).getWriteHoldCount();
  }

  /**
   * Queries if the write lock is held by the current thread for a given
   * resource lock name.
   *
   * @return {@code true} if the current thread holds the write lock and
   *         {@code false} otherwise
   */
  @Override
  @VisibleForTesting
  public boolean isWriteLockedByCurrentThread(Resource resource, String... keys) {
    return getResourceLocks(resource).getLockForTesting(resource, keys).isWriteLockedByCurrentThread();
  }

  /**
   * Unregisters OMLockMetrics source.
   */
  @Override
  public void cleanup() {
    omLockMetrics.unRegister();
  }

  @Override
  public OMLockMetrics getOMLockMetrics() {
    return omLockMetrics;
  }

  /**
   * Leveled Resource defined in Ozone.
   * Enforces lock acquisition ordering based on the resource level. A resource at lower level cannot be acquired
   * after a higher level lock is already acquired.
   */
  public enum LeveledResource implements Resource {
    // For S3 Bucket need to allow only for S3, that should be means only 1.
    S3_BUCKET_LOCK((byte) 0, "S3_BUCKET_LOCK"), // = 1

    // For volume need to allow both s3 bucket and volume. 01 + 10 = 11 (3)
    VOLUME_LOCK((byte) 1, "VOLUME_LOCK"), // = 2

    // For bucket we need to allow both s3 bucket, volume and bucket. Which
    // is equal to 100 + 010 + 001 = 111 = 4 + 2 + 1 = 7
    BUCKET_LOCK((byte) 2, "BUCKET_LOCK"), // = 4

    // For user we need to allow s3 bucket, volume, bucket and user lock.
    // Which is 8  4 + 2 + 1 = 15
    USER_LOCK((byte) 3, "USER_LOCK"), // 15

    S3_SECRET_LOCK((byte) 4, "S3_SECRET_LOCK"), // 31
    KEY_PATH_LOCK((byte) 5, "KEY_PATH_LOCK"), //63
    PREFIX_LOCK((byte) 6, "PREFIX_LOCK"), //127
    SNAPSHOT_LOCK((byte) 7, "SNAPSHOT_LOCK"); // = 255

    // This will tell the value, till which we can allow locking.
    private short mask;

    // This value will help during setLock, and also will tell whether we can
    // re-acquire lock or not.
    private short setMask;

    // Name of the resource.
    private String name;

    private ResourceManager resourceManager;

    LeveledResource(byte pos, String name) {
      // level of the resource
      this.mask = (short) (Math.pow(2, pos + 1) - 1);
      this.setMask = (short) Math.pow(2, pos);
      this.name = name;
      this.resourceManager = new ResourceManager();
    }

    boolean canLock(short lockSetVal) {

      // For USER_LOCK, S3_SECRET_LOCK and  PREFIX_LOCK we shall not allow
      // re-acquire locks from single thread. 2nd condition is we have
      // acquired one of these locks, but after that trying to acquire a lock
      // with less than equal of lockLevel, we should disallow.
      if (((USER_LOCK.setMask & lockSetVal) == USER_LOCK.setMask ||
          (S3_SECRET_LOCK.setMask & lockSetVal) == S3_SECRET_LOCK.setMask ||
          (PREFIX_LOCK.setMask & lockSetVal) == PREFIX_LOCK.setMask)
          && setMask <= lockSetVal) {
        return false;
      }


      // Our mask is the summation of bits of all previous possible locks. In
      // other words it is the largest possible value for that bit position.

      // For example for Volume lock, bit position is 1, and mask is 3. Which
      // is the largest value that can be represented with 2 bits is 3.
      // Therefore if lockSet is larger than mask we have to return false i.e
      // some other higher order lock has been acquired.

      return lockSetVal <= mask;
    }

    /**
     * Set Lock bits in lockSetVal.
     *
     * @param lockSetVal
     * @return Updated value which has set lock bits.
     */
    short setLock(short lockSetVal) {
      return (short) (lockSetVal | setMask);
    }

    /**
     * Clear lock from lockSetVal.
     *
     * @param lockSetVal
     * @return Updated value which has cleared lock bits.
     */
    short clearLock(short lockSetVal) {
      return (short) (lockSetVal & ~setMask);
    }

    /**
     * Return true, if this level is locked, else false.
     * @param lockSetVal
     */
    boolean isLevelLocked(short lockSetVal) {
      return (lockSetVal & setMask) == setMask;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public ResourceManager getResourceManager() {
      return resourceManager;
    }

    short getMask() {
      return mask;
    }
  }

  /**
   * Update the processing details.
   *
   * If Server.getCurCall() is null, which means it's write operation on Ratis,
   * then we need to update the omLockDetails.
   * If not null, it's read operation, or write operation on non-Ratis cluster,
   * we can update ThreadLocal variable directly.
   * @param type IPC Timing types
   * @param deltaNanos consumed time
   */
  private void updateProcessingDetails(ResourceLockTracker<? extends Resource> resourceLockTracker, Timing type,
      long deltaNanos) {
    Server.Call call = Server.getCurCall().get();
    if (call != null) {
      call.getProcessingDetails().add(type, deltaNanos, TimeUnit.NANOSECONDS);
    } else {
      switch (type) {
      case LOCKWAIT:
        resourceLockTracker.getOmLockDetails().add(deltaNanos, OMLockDetails.LockOpType.WAIT);
        break;
      case LOCKSHARED:
        resourceLockTracker.getOmLockDetails().add(deltaNanos, OMLockDetails.LockOpType.READ);
        break;
      case LOCKEXCLUSIVE:
        resourceLockTracker.getOmLockDetails().add(deltaNanos, OMLockDetails.LockOpType.WRITE);
        break;
      default:
        LOG.error("Unsupported Timing type {}", type);
      }
    }
  }
}

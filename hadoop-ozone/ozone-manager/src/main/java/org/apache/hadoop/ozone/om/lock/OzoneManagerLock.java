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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.CompositeKey;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.apache.hadoop.ipc_.ProcessingDetails.Timing;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.util.Time;
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
    private final Map<Resource, Striped<ReentrantReadWriteLock>> lockMap;
    private final ResourceLockTracker<R> tracker;

    class Context {
      private final R resource;
      private final long startWaitingTimeNanos = Time.monotonicNowNanos();

      Context(R r) {
        this.resource = r;
      }

      R getResource() {
        return resource;
      }

      long getWaitingTimeNanos() {
        return Time.monotonicNowNanos() - startWaitingTimeNanos;
      }

      ResourceLockTracker<R> getTracker() {
        return tracker;
      }
    }

    ResourceLocks(Map<Resource, Striped<ReentrantReadWriteLock>> lockMap, ResourceLockTracker<R> tracker) {
      this.lockMap = lockMap;
      this.tracker = tracker;
    }

    ResourceLockTracker<R> getTracker() {
      return tracker;
    }

    private R cast(Resource resource) {
      if (resource.getClass() != tracker.getResourceClass()) {
        throw new IllegalArgumentException("Unexpected resource " + resource.getClass()
            + " for tracker " + tracker.getResourceClass());
      }
      @SuppressWarnings("unchecked")
      final R r = (R) resource;
      return r;
    }

    Context newContext(Resource resource) {
      final R r = cast(resource);
      tracker.clearLockDetails();
      if (!tracker.canLockResource(r)) {
        final String errorMessage =  "Thread '" + Thread.currentThread().getName() + "' cannot acquire "
            + resource.getName() + " lock while holding " + getCurrentLocks() + " lock(s).";
        LOG.error(errorMessage);
        // TODO: change it to IllegalStateException
        throw new RuntimeException(errorMessage);
      }
      return new Context(r);
    }

    private ReentrantReadWriteLock getLock(Resource resource, String... keys) {
      return lockMap.get(resource).get(CompositeKey.combineKeys(keys));
    }

    private void acquireLock(ResourceLocks<?>.Context context, boolean isRead, ReentrantReadWriteLock lock) {
      if (isRead) {
        lock.readLock().lock();
        updateReadLockMetrics(context, lock);
      } else {
        lock.writeLock().lock();
        updateWriteLockMetrics(context, lock);
      }
    }

    OMLockDetails acquire(Resource resource, boolean isRead,
        Function<Striped<ReentrantReadWriteLock>, Iterable<ReentrantReadWriteLock>> getLocks) {
      final Context context = newContext(resource);
      for (ReentrantReadWriteLock lock : getLocks.apply(lockMap.get(resource))) {
        acquireLock(context, isRead, lock);
      }
      return tracker.lockResource(context.getResource());
    }

    OMLockDetails acquire(Resource resource, boolean isRead, String... keys) {
      final Context context = newContext(resource);
      acquireLock(context, isRead, getLock(resource, keys));
      return tracker.lockResource(context.getResource());
    }

    void releaseLock(Resource resource, boolean isRead, ReentrantReadWriteLock lock) {
      if (isRead) {
        lock.readLock().unlock();
        updateReadUnlockMetrics(resource, lock);
      } else {
        boolean isWriteLocked = lock.isWriteLockedByCurrentThread();
        lock.writeLock().unlock();
        updateWriteUnlockMetrics(resource, lock, isWriteLocked);
      }
    }

    OMLockDetails release(Resource resource, boolean isRead, String... keys) {
      tracker.clearLockDetails();
      ReentrantReadWriteLock lock = getLock(resource, keys);
      releaseLock(resource, isRead, lock);
      return tracker.unlockResource(cast(resource));
    }

    private OMLockDetails release(Resource resource, boolean isRead,
        Function<Striped<ReentrantReadWriteLock>, Iterable<ReentrantReadWriteLock>> getLock) {
      tracker.clearLockDetails();
      final Iterable<ReentrantReadWriteLock> i = getLock.apply(lockMap.get(resource));
      final List<ReentrantReadWriteLock> locks = StreamSupport.stream(i.spliterator(), false)
          .collect(Collectors.toList());
      // Release locks in reverse order.
      Collections.reverse(locks);
      for (ReentrantReadWriteLock lock : locks) {
        releaseLock(resource, isRead, lock);
      }
      return tracker.unlockResource(cast(resource));
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

  private  <T extends Resource> ResourceLocks<T> cast(ResourceLocks<?> resourceLocks) {
    @SuppressWarnings("unchecked")
    final ResourceLocks<T> locks = (ResourceLocks<T>) resourceLocks;
    return locks;
  }

  private <T extends Resource> ResourceLocks<T> getResourceLocks(Resource instance) {
    final Class<?> clazz = instance.getClass();
    if (clazz == LeveledResource.class) {
      return cast(leveledResourceLocks);
    } else if (clazz == DAGLeveledResource.class) {
      return cast(dagLeveledResourceLocks);
    }
    throw new IllegalArgumentException("Unsupported resource class: " + clazz);
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

  private static Striped<ReentrantReadWriteLock> createStripeLock(Resource r, ConfigurationSource conf) {
    boolean fair = conf.getBoolean(OZONE_MANAGER_FAIR_LOCK,
        OZONE_MANAGER_FAIR_LOCK_DEFAULT);
    String stripeSizeKey = OZONE_MANAGER_STRIPED_LOCK_SIZE_PREFIX +
        r.getName().toLowerCase();
    int size = conf.getInt(stripeSizeKey,
        OZONE_MANAGER_STRIPED_LOCK_SIZE_DEFAULT);
    return SimpleStriped.readWriteLock(size, fair);
  }

  private Iterable<ReentrantReadWriteLock> getAllLocks(Striped<ReentrantReadWriteLock> striped) {
    return IntStream.range(0, striped.size()).mapToObj(striped::getAt).collect(Collectors.toList());
  }

  private Iterable<ReentrantReadWriteLock> bulkGetLock(Striped<ReentrantReadWriteLock> striped,
      Collection<String[]> keys) {
    List<Object> lockKeys = new ArrayList<>(keys.size());
    for (String[] key : keys) {
      if (Objects.nonNull(key)) {
        lockKeys.add(CompositeKey.combineKeys(key));
      }
    }
    return striped.bulkGet(lockKeys);
  }

  /**
   * Acquire read lock on resource.
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
   * @param keys - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  @Override
  public OMLockDetails acquireReadLock(Resource resource, String... keys) {
    return getResourceLocks(resource)
        .acquire(resource, true, keys);
  }

  /**
   * Acquire read locks on a list of resources.
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
   * @param keys - A list of Resource names on which user want to acquire locks.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  @Override
  public OMLockDetails acquireReadLocks(Resource resource, Collection<String[]> keys) {
    return getResourceLocks(resource)
        .acquire(resource, true, striped -> bulkGetLock(striped, keys));
  }

  /**
   * Acquire write lock on resource.
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
   * @param keys - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  @Override
  public OMLockDetails acquireWriteLock(Resource resource, String... keys) {
    return getResourceLocks(resource)
        .acquire(resource, false, keys);
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
  public OMLockDetails acquireWriteLocks(Resource resource, Collection<String[]> keys) {
    return getResourceLocks(resource)
        .acquire(resource, false, striped -> bulkGetLock(striped, keys));
  }

  /**
   * Acquires all write locks for a specified resource.
   *
   * @param resource The resource for which the write lock is to be acquired.
   */
  @Override
  public OMLockDetails acquireResourceWriteLock(Resource resource) {
    return getResourceLocks(resource)
        .acquire(resource, false, this::getAllLocks);
  }

  private void updateReadLockMetrics(ResourceLocks<?>.Context context, ReentrantReadWriteLock lock) {

    /*
     *  readHoldCount helps in metrics updation only once in case
     *  of reentrant locks.
     */
    if (lock.getReadHoldCount() == 1) {
      final long readLockWaitingTimeNanos = context.getWaitingTimeNanos();

      // Adds a snapshot to the metric readLockWaitingTimeMsStat.
      omLockMetrics.setReadLockWaitingTimeMsStat(
          TimeUnit.NANOSECONDS.toMillis(readLockWaitingTimeNanos));
      updateProcessingDetails(context.getTracker(),
          Timing.LOCKWAIT, readLockWaitingTimeNanos);

      context.getResource().getResourceManager().setStartReadHeldTimeNanos(Time.monotonicNowNanos());
    }
  }

  private void updateWriteLockMetrics(ResourceLocks<?>.Context context, ReentrantReadWriteLock lock) {
    /*
     *  writeHoldCount helps in metrics updation only once in case
     *  of reentrant locks. Metrics are updated only if the write lock is held
     *  by the current thread.
     */
    if ((lock.getWriteHoldCount() == 1) &&
        lock.isWriteLockedByCurrentThread()) {
      long writeLockWaitingTimeNanos = context.getWaitingTimeNanos();

      // Adds a snapshot to the metric writeLockWaitingTimeMsStat.
      omLockMetrics.setWriteLockWaitingTimeMsStat(
          TimeUnit.NANOSECONDS.toMillis(writeLockWaitingTimeNanos));
      updateProcessingDetails(context.getTracker(), Timing.LOCKWAIT,
          writeLockWaitingTimeNanos);

      context.getResource().getResourceManager().setStartWriteHeldTimeNanos(Time.monotonicNowNanos());
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


  /**
   * Release write lock on resource.
   * @param resource - Type of the resource.
   * @param keys - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  @Override
  public OMLockDetails releaseWriteLock(Resource resource, String... keys) {
    return getResourceLocks(resource)
        .release(resource, false, keys);
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
  public OMLockDetails releaseWriteLocks(Resource resource, Collection<String[]> keys) {
    return getResourceLocks(resource)
        .release(resource, false, striped -> bulkGetLock(striped, keys));
  }

  /**
   * Releases a write lock acquired on the entire Stripe for a specified resource.
   *
   * @param resource The resource for which the write lock is to be acquired.
   */
  @Override
  public OMLockDetails releaseResourceWriteLock(Resource resource) {
    return getResourceLocks(resource)
        .release(resource, false, this::getAllLocks);
  }

  /**
   * Release read lock on resource.
   * @param resource - Type of the resource.
   * @param keys - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  @Override
  public OMLockDetails releaseReadLock(Resource resource, String... keys) {
    return getResourceLocks(resource)
        .release(resource, true, keys);
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
  public OMLockDetails releaseReadLocks(Resource resource, Collection<String[]> keys) {
    return getResourceLocks(resource)
        .release(resource, true, striped -> bulkGetLock(striped, keys));
  }

  private void updateReadUnlockMetrics(Resource resource,
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
      updateProcessingDetails(getResourceLocks(resource).getTracker(), Timing.LOCKSHARED,
          readLockHeldTimeNanos);
    }
  }

  private void updateWriteUnlockMetrics(Resource resource,
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
      updateProcessingDetails(getResourceLocks(resource).getTracker(), Timing.LOCKEXCLUSIVE,
          writeLockHeldTimeNanos);
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
    return getResourceLocks(resource).getLock(resource, keys).getReadHoldCount();
  }


  /**
   * Returns writeHoldCount for a given resource lock name.
   *
   * @return writeHoldCount
   */
  @Override
  @VisibleForTesting
  public int getWriteHoldCount(Resource resource, String... keys) {
    return getResourceLocks(resource).getLock(resource, keys).getWriteHoldCount();
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
    return getResourceLocks(resource).getLock(resource, keys).isWriteLockedByCurrentThread();
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

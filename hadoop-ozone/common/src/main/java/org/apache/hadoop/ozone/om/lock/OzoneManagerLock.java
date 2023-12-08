/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.lock;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.apache.hadoop.ipc.ProcessingDetails.Timing;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.conf.ConfigurationSource;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_FAIR_LOCK;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_FAIR_LOCK_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_STRIPED_LOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_STRIPED_LOCK_SIZE_PREFIX;
import static org.apache.hadoop.hdds.utils.CompositeKey.combineKeys;

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

  private final Map<Resource, Striped<ReadWriteLock>> stripedLockByResource;

  private OMLockMetrics omLockMetrics;
  private final ThreadLocal<Short> lockSet = ThreadLocal.withInitial(
      () -> Short.valueOf((short)0));

  private ThreadLocal<OMLockDetails> omLockDetails =
      ThreadLocal.withInitial(OMLockDetails::new);

  /**
   * Creates new OzoneManagerLock instance.
   * @param conf Configuration object
   */
  public OzoneManagerLock(ConfigurationSource conf) {
    omLockMetrics = OMLockMetrics.create();
    Map<Resource, Striped<ReadWriteLock>> stripedLockMap =
        new EnumMap<>(Resource.class);
    for (Resource r : Resource.values()) {
      stripedLockMap.put(r, createStripeLock(r, conf));
    }
    this.stripedLockByResource = Collections.unmodifiableMap(stripedLockMap);
  }

  private Striped<ReadWriteLock> createStripeLock(Resource r,
      ConfigurationSource conf) {
    boolean fair = conf.getBoolean(OZONE_MANAGER_FAIR_LOCK,
        OZONE_MANAGER_FAIR_LOCK_DEFAULT);
    String stripeSizeKey = OZONE_MANAGER_STRIPED_LOCK_SIZE_PREFIX +
        r.getName().toLowerCase();
    int size = conf.getInt(stripeSizeKey,
        OZONE_MANAGER_STRIPED_LOCK_SIZE_DEFAULT);
    return SimpleStriped.readWriteLock(size, fair);
  }

  private ReentrantReadWriteLock getLock(Resource resource, String... keys) {
    Striped<ReadWriteLock> striped = stripedLockByResource.get(resource);
    Object key = combineKeys(keys);
    return (ReentrantReadWriteLock) striped.get(key);
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
    return acquireLock(resource, true, keys);
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
    return acquireLock(resource, false, keys);
  }

  private OMLockDetails acquireLock(Resource resource, boolean isReadLock,
      String... keys) {
    omLockDetails.get().clear();
    if (!resource.canLock(lockSet.get())) {
      String errorMessage = getErrorMessage(resource);
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    }

    long startWaitingTimeNanos = Time.monotonicNowNanos();

    ReentrantReadWriteLock lock = getLock(resource, keys);
    if (isReadLock) {
      lock.readLock().lock();
      updateReadLockMetrics(resource, lock, startWaitingTimeNanos);
    } else {
      lock.writeLock().lock();
      updateWriteLockMetrics(resource, lock, startWaitingTimeNanos);
    }

    lockSet.set(resource.setLock(lockSet.get()));
    omLockDetails.get().setLockAcquired(true);
    return omLockDetails.get();
  }

  private void updateReadLockMetrics(Resource resource,
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
      updateProcessingDetails(Timing.LOCKWAIT, readLockWaitingTimeNanos);

      resource.setStartReadHeldTimeNanos(Time.monotonicNowNanos());
    }
  }

  private void updateWriteLockMetrics(Resource resource,
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
      updateProcessingDetails(Timing.LOCKWAIT, writeLockWaitingTimeNanos);

      resource.setStartWriteHeldTimeNanos(Time.monotonicNowNanos());
    }
  }

  private String getErrorMessage(Resource resource) {
    return "Thread '" + Thread.currentThread().getName() + "' cannot " +
        "acquire " + resource.name + " lock while holding " +
        getCurrentLocks().toString() + " lock(s).";

  }

  @VisibleForTesting
  List<String> getCurrentLocks() {
    List<String> currentLocks = new ArrayList<>();
    short lockSetVal = lockSet.get();
    for (Resource value : Resource.values()) {
      if (value.isLevelLocked(lockSetVal)) {
        currentLocks.add(value.getName());
      }
    }
    return currentLocks;
  }

  /**
   * Acquire lock on multiple users.
   */
  @Override
  public boolean acquireMultiUserLock(String firstUser, String secondUser) {
    Resource resource = Resource.USER_LOCK;

    if (!resource.canLock(lockSet.get())) {
      String errorMessage = getErrorMessage(resource);
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      Striped<ReadWriteLock> striped =
          stripedLockByResource.get(Resource.USER_LOCK);
      // The result of bulkGet is always sorted in a consistent order.
      // This prevents deadlocks.
      Iterable<ReadWriteLock> locks =
          striped.bulkGet(Arrays.asList(firstUser, secondUser));
      for (ReadWriteLock lock : locks) {
        lock.writeLock().lock();
      }

      lockSet.set(resource.setLock(lockSet.get()));
      return true;
    }
  }


  /**
   * Release lock on multiple users.
   * @param firstUser
   * @param secondUser
   */
  @Override
  public void releaseMultiUserLock(String firstUser, String secondUser) {
    Striped<ReadWriteLock> striped =
        stripedLockByResource.get(Resource.USER_LOCK);
    Iterable<ReadWriteLock> locks =
        striped.bulkGet(Arrays.asList(firstUser, secondUser));
    for (ReadWriteLock lock : locks) {
      lock.writeLock().unlock();
    }

    lockSet.set(Resource.USER_LOCK.clearLock(lockSet.get()));
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
    return releaseLock(resource, false, keys);
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
    return releaseLock(resource, true, keys);
  }

  private OMLockDetails releaseLock(Resource resource, boolean isReadLock,
      String... keys) {
    omLockDetails.get().clear();
    ReentrantReadWriteLock lock = getLock(resource, keys);
    if (isReadLock) {
      lock.readLock().unlock();
      updateReadUnlockMetrics(resource, lock);
    } else {
      boolean isWriteLocked = lock.isWriteLockedByCurrentThread();
      lock.writeLock().unlock();
      updateWriteUnlockMetrics(resource, lock, isWriteLocked);
    }

    lockSet.set(resource.clearLock(lockSet.get()));
    return omLockDetails.get();
  }

  private void updateReadUnlockMetrics(Resource resource,
      ReentrantReadWriteLock lock) {
    /*
     *  readHoldCount helps in metrics updation only once in case
     *  of reentrant locks.
     */
    if (lock.getReadHoldCount() == 0) {
      long readLockHeldTimeNanos =
          Time.monotonicNowNanos() - resource.getStartReadHeldTimeNanos();

      // Adds a snapshot to the metric readLockHeldTimeMsStat.
      omLockMetrics.setReadLockHeldTimeMsStat(
          TimeUnit.NANOSECONDS.toMillis(readLockHeldTimeNanos));
      updateProcessingDetails(Timing.LOCKSHARED, readLockHeldTimeNanos);
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
          Time.monotonicNowNanos() - resource.getStartWriteHeldTimeNanos();

      // Adds a snapshot to the metric writeLockHeldTimeMsStat.
      omLockMetrics.setWriteLockHeldTimeMsStat(
          TimeUnit.NANOSECONDS.toMillis(writeLockHeldTimeNanos));
      updateProcessingDetails(Timing.LOCKEXCLUSIVE, writeLockHeldTimeNanos);
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
    return getLock(resource, keys).getReadHoldCount();
  }


  /**
   * Returns writeHoldCount for a given resource lock name.
   *
   * @return writeHoldCount
   */
  @Override
  @VisibleForTesting
  public int getWriteHoldCount(Resource resource, String... keys) {
    return getLock(resource, keys).getWriteHoldCount();
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
  public boolean isWriteLockedByCurrentThread(Resource resource,
      String... keys) {
    return getLock(resource, keys).isWriteLockedByCurrentThread();
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
   * Resource defined in Ozone.
   */
  public enum Resource {
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

    // level of the resource
    private byte lockLevel;

    // This will tell the value, till which we can allow locking.
    private short mask;

    // This value will help during setLock, and also will tell whether we can
    // re-acquire lock or not.
    private short setMask;

    // Name of the resource.
    private String name;

    // This helps in maintaining read lock related variables locally confined
    // to a given thread.
    private final ThreadLocal<LockUsageInfo> readLockTimeStampNanos =
        ThreadLocal.withInitial(LockUsageInfo::new);

    // This helps in maintaining write lock related variables locally confined
    // to a given thread.
    private final ThreadLocal<LockUsageInfo> writeLockTimeStampNanos =
        ThreadLocal.withInitial(LockUsageInfo::new);

    /**
     * Sets the time (ns) when the read lock holding period begins specific to a
     * thread.
     *
     * @param startReadHeldTimeNanos read lock held start time (ns)
     */
    void setStartReadHeldTimeNanos(long startReadHeldTimeNanos) {
      readLockTimeStampNanos.get()
          .setStartReadHeldTimeNanos(startReadHeldTimeNanos);
    }

    /**
     * Sets the time (ns) when the write lock holding period begins specific to
     * a thread.
     *
     * @param startWriteHeldTimeNanos write lock held start time (ns)
     */
    void setStartWriteHeldTimeNanos(long startWriteHeldTimeNanos) {
      writeLockTimeStampNanos.get()
          .setStartWriteHeldTimeNanos(startWriteHeldTimeNanos);
    }

    /**
     * Returns the time (ns) when the read lock holding period began specific to
     * a thread.
     *
     * @return read lock held start time (ns)
     */
    long getStartReadHeldTimeNanos() {
      long startReadHeldTimeNanos =
          readLockTimeStampNanos.get().getStartReadHeldTimeNanos();
      readLockTimeStampNanos.remove();
      return startReadHeldTimeNanos;
    }

    /**
     * Returns the time (ns) when the write lock holding period began specific
     * to a thread.
     *
     * @return write lock held start time (ns)
     */
    long getStartWriteHeldTimeNanos() {
      long startWriteHeldTimeNanos =
          writeLockTimeStampNanos.get().getStartWriteHeldTimeNanos();
      writeLockTimeStampNanos.remove();
      return startWriteHeldTimeNanos;
    }

    Resource(byte pos, String name) {
      this.lockLevel = pos;
      this.mask = (short) (Math.pow(2, lockLevel + 1) - 1);
      this.setMask = (short) Math.pow(2, lockLevel);
      this.name = name;
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

    String getName() {
      return name;
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
  private void updateProcessingDetails(Timing type, long deltaNanos) {
    Server.Call call = Server.getCurCall().get();
    if (call != null) {
      call.getProcessingDetails().add(type, deltaNanos, TimeUnit.NANOSECONDS);
    } else {
      switch (type) {
      case LOCKWAIT:
        omLockDetails.get().add(deltaNanos, OMLockDetails.LockOpType.WAIT);
        break;
      case LOCKSHARED:
        omLockDetails.get().add(deltaNanos, OMLockDetails.LockOpType.READ);
        break;
      case LOCKEXCLUSIVE:
        omLockDetails.get().add(deltaNanos, OMLockDetails.LockOpType.WRITE);
        break;
      default:
        LOG.error("Unsupported Timing type {}", type);
      }
    }
  }
}

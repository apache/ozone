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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.lock.LockManager;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_FAIR_LOCK_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_FAIR_LOCK;

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

public class OzoneManagerLock {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerLock.class);

  private static final String READ_LOCK = "read";
  private static final String WRITE_LOCK = "write";

  private final LockManager<String> manager;
  private OMLockMetrics omLockMetrics;
  private final ThreadLocal<Short> lockSet = ThreadLocal.withInitial(
      () -> Short.valueOf((short)0));

  /**
   * Creates new OzoneManagerLock instance.
   * @param conf Configuration object
   */
  public OzoneManagerLock(ConfigurationSource conf) {
    boolean fair = conf.getBoolean(OZONE_MANAGER_FAIR_LOCK,
        OZONE_MANAGER_FAIR_LOCK_DEFAULT);
    manager = new LockManager<>(conf, fair);
    omLockMetrics = OMLockMetrics.create();
  }

  /**
   * Acquire lock on resource.
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
   * @param resources - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  @Deprecated
  public boolean acquireLock(Resource resource, String... resources) {
    String resourceName = generateResourceName(resource, resources);
    return lock(resource, resourceName, manager::writeLock, WRITE_LOCK);
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
   * @param resources - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  public boolean acquireReadLock(Resource resource, String... resources) {
    String resourceName = generateResourceName(resource, resources);
    return lock(resource, resourceName, manager::readLock, READ_LOCK);
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
   * @param resources - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  public boolean acquireWriteLock(Resource resource, String... resources) {
    String resourceName = generateResourceName(resource, resources);
    return lock(resource, resourceName, manager::writeLock, WRITE_LOCK);
  }

  private boolean lock(Resource resource, String resourceName,
      Consumer<String> lockFn, String lockType) {
    if (!resource.canLock(lockSet.get())) {
      String errorMessage = getErrorMessage(resource);
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      long startWaitingTimeNanos = Time.monotonicNowNanos();
      lockFn.accept(resourceName);

      /**
       *  read/write lock hold count helps in metrics updation only once in case
       *  of reentrant locks.
       */
      if (lockType.equals(READ_LOCK) &&
          manager.getReadHoldCount(resourceName) == 1) {
        updateReadLockMetrics(resource, startWaitingTimeNanos);
      }
      if (lockType.equals(WRITE_LOCK) &&
          (manager.getWriteHoldCount(resourceName) == 1) &&
          manager.isWriteLockedByCurrentThread(resourceName)) {
        updateWriteLockMetrics(resource, startWaitingTimeNanos);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Acquired {} {} lock on resource {}", lockType, resource.name,
            resourceName);
      }
      lockSet.set(resource.setLock(lockSet.get()));
      return true;
    }
  }

  private void updateReadLockMetrics(Resource resource,
                                     long startWaitingTimeNanos) {
    long readLockWaitingTimeNanos =
        Time.monotonicNowNanos() - startWaitingTimeNanos;

    // Adds a snapshot to the metric readLockWaitingTimeMsStat.
    omLockMetrics.setReadLockWaitingTimeMsStat(
        TimeUnit.NANOSECONDS.toMillis(readLockWaitingTimeNanos));

    resource.setStartReadHeldTimeNanos(Time.monotonicNowNanos());
  }

  private void updateWriteLockMetrics(Resource resource,
                                      long startWaitingTimeNanos) {
    long writeLockWaitingTimeNanos =
        Time.monotonicNowNanos() - startWaitingTimeNanos;

    // Adds a snapshot to the metric writeLockWaitingTimeMsStat.
    omLockMetrics.setWriteLockWaitingTimeMsStat(
        TimeUnit.NANOSECONDS.toMillis(writeLockWaitingTimeNanos));

    resource.setStartWriteHeldTimeNanos(Time.monotonicNowNanos());
  }

  /**
   * Generate resource name to be locked.
   * @param resource
   * @param resources
   */
  private String generateResourceName(Resource resource, String... resources) {
    if (resources.length == 1 && resource != Resource.BUCKET_LOCK) {
      return OzoneManagerLockUtil.generateResourceLockName(resource,
          resources[0]);
    } else if (resources.length == 2 && resource == Resource.BUCKET_LOCK) {
      return OzoneManagerLockUtil.generateBucketLockName(resources[0],
          resources[1]);
    } else {
      throw new IllegalArgumentException("acquire lock is supported on single" +
          " resource for all locks except for resource bucket");
    }
  }

  private String getErrorMessage(Resource resource) {
    return "Thread '" + Thread.currentThread().getName() + "' cannot " +
        "acquire " + resource.name + " lock while holding " +
        getCurrentLocks().toString() + " lock(s).";

  }

  private List<String> getCurrentLocks() {
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
   * @param firstUser
   * @param secondUser
   */
  public boolean acquireMultiUserLock(String firstUser, String secondUser) {
    Resource resource = Resource.USER_LOCK;
    firstUser = generateResourceName(resource, firstUser);
    secondUser = generateResourceName(resource, secondUser);

    if (!resource.canLock(lockSet.get())) {
      String errorMessage = getErrorMessage(resource);
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      // When acquiring multiple user locks, the reason for doing lexical
      // order comparison is to avoid deadlock scenario.

      // Example: 1st thread acquire lock(ozone, hdfs)
      // 2nd thread acquire lock(hdfs, ozone).
      // If we don't acquire user locks in an order, there can be a deadlock.
      // 1st thread acquired lock on ozone, waiting for lock on hdfs, 2nd
      // thread acquired lock on hdfs, waiting for lock on ozone.
      // To avoid this when we acquire lock on multiple users, we acquire
      // locks in lexical order, which can help us to avoid dead locks.
      // Now if first thread acquires lock on hdfs, 2nd thread wait for lock
      // on hdfs, and first thread acquires lock on ozone. Once after first
      // thread releases user locks, 2nd thread acquires them.

      int compare = firstUser.compareTo(secondUser);
      String temp;

      // Order the user names in sorted order. Swap them.
      if (compare > 0) {
        temp = secondUser;
        secondUser = firstUser;
        firstUser = temp;
      }

      if (compare == 0) {
        // both users are equal.
        manager.writeLock(firstUser);
      } else {
        manager.writeLock(firstUser);
        try {
          manager.writeLock(secondUser);
        } catch (Exception ex) {
          // We got an exception acquiring 2nd user lock. Release already
          // acquired user lock, and throw exception to the user.
          manager.writeUnlock(firstUser);
          throw ex;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Acquired Write {} lock on resource {} and {}", resource.name,
            firstUser, secondUser);
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
  public void releaseMultiUserLock(String firstUser, String secondUser) {
    Resource resource = Resource.USER_LOCK;
    firstUser = generateResourceName(resource, firstUser);
    secondUser = generateResourceName(resource, secondUser);

    int compare = firstUser.compareTo(secondUser);

    String temp;
    // Order the user names in sorted order. Swap them.
    if (compare > 0) {
      temp = secondUser;
      secondUser = firstUser;
      firstUser = temp;
    }

    if (compare == 0) {
      // both users are equal.
      manager.writeUnlock(firstUser);
    } else {
      manager.writeUnlock(firstUser);
      manager.writeUnlock(secondUser);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Release Write {} lock on resource {} and {}", resource.name,
          firstUser, secondUser);
    }
    lockSet.set(resource.clearLock(lockSet.get()));
  }

  /**
   * Release write lock on resource.
   * @param resource - Type of the resource.
   * @param resources - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  public void releaseWriteLock(Resource resource, String... resources) {
    String resourceName = generateResourceName(resource, resources);
    unlock(resource, resourceName, manager::writeUnlock, WRITE_LOCK);
  }

  /**
   * Release read lock on resource.
   * @param resource - Type of the resource.
   * @param resources - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  public void releaseReadLock(Resource resource, String... resources) {
    String resourceName = generateResourceName(resource, resources);
    unlock(resource, resourceName, manager::readUnlock, READ_LOCK);
  }

  /**
   * Release write lock on resource.
   * @param resource - Type of the resource.
   * @param resources - Resource names on which user want to acquire lock.
   * For Resource type BUCKET_LOCK, first param should be volume, second param
   * should be bucket name. For remaining all resource only one param should
   * be passed.
   */
  @Deprecated
  public void releaseLock(Resource resource, String... resources) {
    String resourceName = generateResourceName(resource, resources);
    unlock(resource, resourceName, manager::writeUnlock, WRITE_LOCK);
  }

  private void unlock(Resource resource, String resourceName,
      Consumer<String> lockFn, String lockType) {
    boolean isWriteLocked = manager.isWriteLockedByCurrentThread(resourceName);
    // TODO: Not checking release of higher order level lock happened while
    // releasing lower order level lock, as for that we need counter for
    // locks, as some locks support acquiring lock again.
    lockFn.accept(resourceName);

    /**
     *  read/write lock hold count helps in metrics updation only once in case
     *  of reentrant locks.
     */
    if (lockType.equals(READ_LOCK) &&
        manager.getReadHoldCount(resourceName) == 0) {
      updateReadUnlockMetrics(resource);
    }
    if (lockType.equals(WRITE_LOCK) &&
        (manager.getWriteHoldCount(resourceName) == 0) && isWriteLocked) {
      updateWriteUnlockMetrics(resource);
    }

    // clear lock
    if (LOG.isDebugEnabled()) {
      LOG.debug("Release {} {}, lock on resource {}", lockType, resource.name,
          resourceName);
    }
    lockSet.set(resource.clearLock(lockSet.get()));
  }

  private void updateReadUnlockMetrics(Resource resource) {
    long readLockHeldTimeNanos =
        Time.monotonicNowNanos() - resource.getStartReadHeldTimeNanos();

    // Adds a snapshot to the metric readLockHeldTimeMsStat.
    omLockMetrics.setReadLockHeldTimeMsStat(
        TimeUnit.NANOSECONDS.toMillis(readLockHeldTimeNanos));
  }

  private void updateWriteUnlockMetrics(Resource resource) {
    long writeLockHeldTimeNanos =
        Time.monotonicNowNanos() - resource.getStartWriteHeldTimeNanos();

    // Adds a snapshot to the metric writeLockHeldTimeMsStat.
    omLockMetrics.setWriteLockHeldTimeMsStat(
        TimeUnit.NANOSECONDS.toMillis(writeLockHeldTimeNanos));
  }

  /**
   * Returns readHoldCount for a given resource lock name.
   *
   * @param resourceName resource lock name
   * @return readHoldCount
   */
  @VisibleForTesting
  public int getReadHoldCount(String resourceName) {
    return manager.getReadHoldCount(resourceName);
  }

  /**
   * Returns a string representation of the object. Provides information on the
   * total number of samples, minimum value, maximum value, arithmetic mean,
   * standard deviation of all the samples added.
   *
   * @return String representation of object
   */
  @VisibleForTesting
  public String getReadLockWaitingTimeMsStat() {
    return omLockMetrics.getReadLockWaitingTimeMsStat();
  }

  /**
   * Returns the longest time (ms) a read lock was waiting since the last
   * measurement.
   *
   * @return longest read lock waiting time (ms)
   */
  @VisibleForTesting
  public long getLongestReadLockWaitingTimeMs() {
    return omLockMetrics.getLongestReadLockWaitingTimeMs();
  }

  /**
   * Returns a string representation of the object. Provides information on the
   * total number of samples, minimum value, maximum value, arithmetic mean,
   * standard deviation of all the samples added.
   *
   * @return String representation of object
   */
  @VisibleForTesting
  public String getReadLockHeldTimeMsStat() {
    return omLockMetrics.getReadLockHeldTimeMsStat();
  }

  /**
   * Returns the longest time (ms) a read lock was held since the last
   * measurement.
   *
   * @return longest read lock held time (ms)
   */
  @VisibleForTesting
  public long getLongestReadLockHeldTimeMs() {
    return omLockMetrics.getLongestReadLockHeldTimeMs();
  }

  /**
   * Returns writeHoldCount for a given resource lock name.
   *
   * @param resourceName resource lock name
   * @return writeHoldCount
   */
  @VisibleForTesting
  public int getWriteHoldCount(String resourceName) {
    return manager.getWriteHoldCount(resourceName);
  }

  /**
   * Queries if the write lock is held by the current thread for a given
   * resource lock name.
   *
   * @param resourceName resource lock name
   * @return {@code true} if the current thread holds the write lock and
   *         {@code false} otherwise
   */
  @VisibleForTesting
  public boolean isWriteLockedByCurrentThread(String resourceName) {
    return manager.isWriteLockedByCurrentThread(resourceName);
  }

  /**
   * Returns a string representation of the object. Provides information on the
   * total number of samples, minimum value, maximum value, arithmetic mean,
   * standard deviation of all the samples added.
   *
   * @return String representation of object
   */
  @VisibleForTesting
  public String getWriteLockWaitingTimeMsStat() {
    return omLockMetrics.getWriteLockWaitingTimeMsStat();
  }

  /**
   * Returns the longest time (ms) a write lock was waiting since the last
   * measurement.
   *
   * @return longest write lock waiting time (ms)
   */
  @VisibleForTesting
  public long getLongestWriteLockWaitingTimeMs() {
    return omLockMetrics.getLongestWriteLockWaitingTimeMs();
  }

  /**
   * Returns a string representation of the object. Provides information on the
   * total number of samples, minimum value, maximum value, arithmetic mean,
   * standard deviation of all the samples added.
   *
   * @return String representation of object
   */
  @VisibleForTesting
  public String getWriteLockHeldTimeMsStat() {
    return omLockMetrics.getWriteLockHeldTimeMsStat();
  }

  /**
   * Returns the longest time (ms) a write lock was held since the last
   * measurement.
   *
   * @return longest write lock held time (ms)
   */
  @VisibleForTesting
  public long getLongestWriteLockHeldTimeMs() {
    return omLockMetrics.getLongestWriteLockHeldTimeMs();
  }

  /**
   * Unregisters OMLockMetrics source.
   */
  public void cleanup() {
    omLockMetrics.unRegister();
  }

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
    PREFIX_LOCK((byte) 5, "PREFIX_LOCK"); //63

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
}

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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.lock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * key locking.
 */
public class OmLockOpr {
  private static final Logger LOG = LoggerFactory.getLogger(OmLockOpr.class);
  private static final long MONITOR_DELAY = 10 * 60 * 1000;
  private static KeyLocking keyLocking;
  // volume locking needs separate as different lock key type can have same name
  private static KeyLocking volumeLocking;
  private static KeyLocking bucketLocking;
  private static KeyLocking snapshotLocking;
  private static FSOPrefixLocking prefixLocking;
  private static Map<OmLockOpr, OmLockOpr> lockedObjMap;
  private static ScheduledExecutorService executorService;

  private LockType type;
  private String volumeName;
  private String bucketName;
  private String keyName;
  private List<String> keyNameList;
  private List<List<FSOPrefixLocking.LockInfo>> lockedKeyFsoList = null;
  private List<String> lockedParentList = null;
  private OMLockDetails lockDetails = new OMLockDetails();
  private long lockTakenTime = 0;

  public OmLockOpr() {
    this.type = LockType.NONE;
  }

  public OmLockOpr(LockType type, String keyName) {
    assert (type == LockType.W_VOLUME || type == LockType.R_VOLUME || type == LockType.W_BUCKET
        || type == LockType.WRITE);
    this.type = type;
    this.keyName = keyName;
  }
  public OmLockOpr(LockType type, String volumeName, String bucketName) {
    assert (type == LockType.RW_VOLUME_BUCKET);
    this.type = type;
    this.volumeName = volumeName;
    this.bucketName = bucketName;
  }
  public OmLockOpr(LockType type, String volume, String bucket, String keyName) {
    this.type = type;
    this.volumeName = volume;
    this.bucketName = bucket;
    this.keyName = keyName;
  }

  public OmLockOpr(LockType type, String volume, String bucket, List<String> keyNameList) {
    this.type = type;
    this.volumeName = volume;
    this.bucketName = bucket;
    this.keyNameList = new ArrayList<>(keyNameList);
    Collections.sort(this.keyNameList);
  }
  public static void init(String threadNamePrefix) {
    keyLocking = new KeyLocking();
    volumeLocking = new KeyLocking();
    snapshotLocking = new KeyLocking();
    bucketLocking = new KeyLocking();
    prefixLocking = new FSOPrefixLocking(threadNamePrefix);
    lockedObjMap = new ConcurrentHashMap<>();
    // init scheduler to check and monitor
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(threadNamePrefix + "OmLockOpr-Monitor-%d").build();
    executorService = Executors.newScheduledThreadPool(1, threadFactory);
    executorService.scheduleWithFixedDelay(() -> monitor(), 0, MONITOR_DELAY, TimeUnit.MILLISECONDS);
  }
  
  public static void stop() {
    executorService.shutdown();
    prefixLocking.stop();
  }

  public static void monitor() {
    LOG.info("FSO: " + prefixLocking.toString());
    LOG.info("Volume: " + volumeLocking.toString());
    LOG.info("Snapshot: " + snapshotLocking.toString());
    LOG.info("Key: " + keyLocking.toString());
    LOG.info("Key: " + bucketLocking.toString());
    LOG.info("Lock operation Status crossing threshold (10 minutes):");
    long startTime = Time.monotonicNowNanos();
    for (Map.Entry<OmLockOpr, OmLockOpr> entry : lockedObjMap.entrySet()) {
      if ((startTime - entry.getKey().getLockTakenTime()) > MONITOR_DELAY) {
        LOG.info("Lock is hold for {}", entry.getKey());
      }
    }
  }

  private long getLockTakenTime() {
    return lockTakenTime;
  }

  public void lock(OzoneManager om) throws IOException {
    long startTime, endTime;
    switch (type) {
    case W_FSO:
      startTime = Time.monotonicNowNanos();
      bucketLocking.readLock(bucketName);
      endTime = Time.monotonicNowNanos();
      lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
      lockedKeyFsoList = new ArrayList<>();
      if (keyName != null) {
        lockedKeyFsoList.add(lockFso(om.getMetadataManager(), volumeName, bucketName, keyName, lockDetails));
      } else {
        for (String key : keyNameList) {
          lockedKeyFsoList.add(lockFso(om.getMetadataManager(), volumeName, bucketName, key, lockDetails));
        }
      }
      break;
    case W_OBS:
      startTime = Time.monotonicNowNanos();
      bucketLocking.readLock(bucketName);
      if (null != keyName) {
        keyLocking.lock(keyName);
      } else {
        keyLocking.lock(keyNameList);
      }
      endTime = Time.monotonicNowNanos();
      lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
      break;
    case W_LEGACY_FSO:
      startTime = Time.monotonicNowNanos();
      bucketLocking.readLock(bucketName);
      endTime = Time.monotonicNowNanos();
      lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
      lockedParentList = getMissingParentPathList(om.getMetadataManager(), volumeName, bucketName, keyName);
      startTime = Time.monotonicNowNanos();
      keyLocking.lock(lockedParentList);
      endTime = Time.monotonicNowNanos();
      lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
      break;
    case SNAPSHOT:
      startTime = Time.monotonicNowNanos();
      bucketLocking.readLock(bucketName);
      if (keyName != null) {
        snapshotLocking.lock(keyName);
      } else {
        snapshotLocking.lock(keyNameList);
      }
      endTime = Time.monotonicNowNanos();
      lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
      break;
    case RW_VOLUME_BUCKET:
      startTime = Time.monotonicNowNanos();
      volumeLocking.readLock(volumeName);
      bucketLocking.lock(bucketName);
      endTime = Time.monotonicNowNanos();
      lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
      break;
    case W_VOLUME:
      startTime = Time.monotonicNowNanos();
      volumeLocking.lock(keyName);
      endTime = Time.monotonicNowNanos();
      lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
      break;
    case R_VOLUME:
      startTime = Time.monotonicNowNanos();
      volumeLocking.readLock(keyName);
      endTime = Time.monotonicNowNanos();
      lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
      break;
    case W_BUCKET:
      startTime = Time.monotonicNowNanos();
      bucketLocking.lock(keyName);
      endTime = Time.monotonicNowNanos();
      lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
      break;
    case WRITE:
      startTime = Time.monotonicNowNanos();
      keyLocking.lock(keyName);
      endTime = Time.monotonicNowNanos();
      lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
      break;
    case NONE:
      break;
    default:
      LOG.error("Invalid Lock Operation type {}", type);
    }
    lockTakenTime = Time.monotonicNowNanos();
    lockDetails.setLockAcquired(true);
    lockedObjMap.put(this, this);
  }

  public void unlock() {
    if (!lockDetails.isLockAcquired()) {
      return;
    }
    lockDetails.setLockAcquired(false);
    switch (type) {
    case W_FSO:
      for (List<FSOPrefixLocking.LockInfo> lockedFsoList : lockedKeyFsoList) {
        unlockFso(lockedFsoList);
      }
      bucketLocking.readUnlock(bucketName);
      break;
    case W_OBS:
      if (null != keyName) {
        keyLocking.unlock(keyName);
      } else {
        keyLocking.unlock(keyNameList);
      }
      bucketLocking.readUnlock(bucketName);
      break;
    case W_LEGACY_FSO:
      keyLocking.unlock(lockedParentList);
      bucketLocking.readUnlock(bucketName);
      break;
    case SNAPSHOT:
      if (keyName != null) {
        snapshotLocking.unlock(keyName);
      } else {
        snapshotLocking.unlock(keyNameList);
      }
      bucketLocking.readUnlock(bucketName);
      break;
    case RW_VOLUME_BUCKET:
      bucketLocking.unlock(bucketName);
      volumeLocking.readUnlock(volumeName);
      break;
    case W_VOLUME:
      volumeLocking.unlock(keyName);
      break;
    case R_VOLUME:
      volumeLocking.readUnlock(keyName);
      break;
    case W_BUCKET:
      bucketLocking.unlock(keyName);
      break;
    case WRITE:
      keyLocking.unlock(keyName);
      break;
    case NONE:
      break;
    default:
      LOG.error("Invalid un-lock Operation type {}", type);
    }
    if (lockTakenTime > 0) {
      if (type == LockType.R_VOLUME) {
        lockDetails.add(Time.monotonicNowNanos() - lockTakenTime, OMLockDetails.LockOpType.READ);
      } else {
        lockDetails.add(Time.monotonicNowNanos() - lockTakenTime, OMLockDetails.LockOpType.WRITE);
      }
    }
    lockedObjMap.remove(this);
  }
  private static List<FSOPrefixLocking.LockInfo> lockFso(
      OMMetadataManager omMetadataManager, String volumeName, String bucketName, String keyName,
      OMLockDetails lockDetails) throws IOException {
    List<String> pathList = getPathList(keyName);
    List<String> dirList = pathList.subList(0, pathList.size() - 1);
    long startTime = Time.monotonicNowNanos();
    List<FSOPrefixLocking.LockInfo> locks = prefixLocking.readLock(dirList);
    long endTime = Time.monotonicNowNanos();
    lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
    // validate for path exist
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    final long bucketId = omBucketInfo.getObjectID();
    long parentId = omBucketInfo.getObjectID();
    int splitIndex = -1;
    for (int i = 0; i < dirList.size(); ++i) {
      String dbNodeName = omMetadataManager.getOzonePathKey(volumeId, bucketId, parentId, dirList.get(i));
      OmDirectoryInfo omDirInfo = omMetadataManager.getDirectoryTable().get(dbNodeName);
      if (omDirInfo != null) {
        parentId = omDirInfo.getObjectID();
        continue;
      }
      if (omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).isExist(dbNodeName)) {
        // do not support directory as file in path, take write lock from that point as dir is non-existing
        // and can not traverse more deep
        LOG.debug("dir as file {} exist, take that as split to take write lock for path {}", dbNodeName, keyName);
      }
      splitIndex = i;
      break;
    }
    
    // update lock with write
    startTime = Time.monotonicNowNanos();
    if (splitIndex == 0) {
      prefixLocking.readUnlock(locks);
      locks.clear();
      locks.add(prefixLocking.writeLock(null, dirList.get(splitIndex)));
    } else if (splitIndex != -1) {
      List<FSOPrefixLocking.LockInfo> releaseLocks = locks.subList(splitIndex, dirList.size());
      prefixLocking.readUnlock(releaseLocks);
      locks = locks.subList(0, splitIndex);
      locks.add(prefixLocking.writeLock(locks.get(splitIndex - 1), dirList.get(splitIndex)));
    } else {
      String leafNode = pathList.get(pathList.size() - 1);
      if (locks.isEmpty()) {
        locks.add(prefixLocking.writeLock(null, leafNode));
      } else {
        locks.add(prefixLocking.writeLock(locks.get(locks.size() - 1), leafNode));
      }
    }
    endTime = Time.monotonicNowNanos();
    lockDetails.add(endTime - startTime, OMLockDetails.LockOpType.WAIT);
    return locks;
  }

  private static void unlockFso(List<FSOPrefixLocking.LockInfo> acquiredLocks) {
    if (acquiredLocks.size() == 0) {
      return;
    }
    prefixLocking.writeUnlock(acquiredLocks.get(acquiredLocks.size() - 1));
    if (acquiredLocks.size() > 1) {
      prefixLocking.readUnlock(acquiredLocks.subList(0, acquiredLocks.size() - 1));
    }
  }

  private static List<String> getPathList(String fullPath) {
    List<String> pathList = new ArrayList<>();
    Path path = Paths.get(fullPath);
    Iterator<Path> elements = path.iterator();
    while (elements.hasNext()) {
      pathList.add(elements.next().toString());
    }
    return pathList;
  }

  private static List<String> getMissingParentPathList(
      OMMetadataManager omMetadataManager, String volumeName, String bucketName, String fullPath) throws IOException {
    List<String> pathList = new ArrayList<>();
    Path path = Paths.get(fullPath);
    while (path != null) {
      String pathName = path.toString();
      String dbKeyName = omMetadataManager.getOzoneKey(volumeName, bucketName, pathName);
      String dbDirKeyName = omMetadataManager.getOzoneDirKey(volumeName, bucketName, pathName);
      if (!omMetadataManager.getKeyTable(BucketLayout.LEGACY).isExist(dbKeyName)
          && !omMetadataManager.getKeyTable(BucketLayout.LEGACY).isExist(dbDirKeyName)) {
        pathList.add(pathName);
      }
      path = path.getParent();
    }
    return pathList;
  }

  public OMLockDetails getLockDetails() {
    return lockDetails;
  }

  public String toString() {
    return String.format("type %s, volumeName %s, bucketName %s, keyName %s, keyNameList %s",
        type.name(), volumeName, bucketName, keyName, keyNameList);
  }
  /**
   * lock operatio type.
   */
  public enum LockType {
    NONE,
    // Lock as per FSO structure with last element missing write lock, otherwise read lock
    // volume name, bucket name and either keyName or keyNameList is mandatory
    W_FSO,
    // Lock as per OBS, all elements with write lock
    W_OBS,
    // Lock specific Legacy FSO identifying missing path and having write lock
    W_LEGACY_FSO,
    W_VOLUME,
    R_VOLUME,
    W_BUCKET,
    WRITE,
    // lock volume as read, but bucket as write lock
    RW_VOLUME_BUCKET,
    // separate bucket of snapshot locking with snapshot name write lock
    SNAPSHOT
  }
}

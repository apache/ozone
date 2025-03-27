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

import java.util.List;

/**
 * Lock information.
 */
public final class OmLockInfo {
  /**
   * lock info provider.
   */
  public interface LockInfoProvider {
    default LockLevel getLevel() {
      return LockLevel.NONE;
    }
  }

  /**
   * volume lock info.
   */
  public static class VolumeLockInfo implements LockInfoProvider {
    private final String volumeName;
    private final LockAction action;
    
    public VolumeLockInfo(String volumeName, LockAction volLockType) {
      this.volumeName = volumeName;
      this.action = volLockType;
    }

    public String getVolumeName() {
      return volumeName;
    }

    public LockAction getAction() {
      return action;
    }

    @Override
    public LockLevel getLevel() {
      return LockLevel.VOLUME;
    }
  }

  /**
   * bucket lock info.
   */
  public static class BucketLockInfo implements LockInfoProvider {
    private final VolumeLockInfo volumeLockInfo;
    private final String bucketName;
    private final LockAction action;
  
    public BucketLockInfo(String bucketName, LockAction action) {
      this.volumeLockInfo = null;
      this.bucketName = bucketName;
      this.action = action;
    }

    public BucketLockInfo(String volumeName, LockAction volLockType, String bucketName, LockAction action) {
      this.volumeLockInfo = new VolumeLockInfo(volumeName, volLockType);
      this.bucketName = bucketName;
      this.action = action;
    }

    public VolumeLockInfo getVolumeLockInfo() {
      return volumeLockInfo;
    }

    public String getBucketName() {
      return bucketName;
    }

    public LockAction getAction() {
      return action;
    }

    @Override
    public LockLevel getLevel() {
      return LockLevel.BUCKET;
    }
  }

  /**
   * key lock info.
   */
  public static class KeyLockInfo implements LockInfoProvider {
    private final BucketLockInfo bucketLockInfo;
    private final String key;
    private final LockAction action;

    public KeyLockInfo(String bucketName, LockAction bucketAction, String keyName, LockAction keyAction) {
      this.bucketLockInfo = new BucketLockInfo(bucketName, bucketAction);
      this.key = keyName;
      this.action = keyAction;
    }

    public BucketLockInfo getBucketLockInfo() {
      return bucketLockInfo;
    }

    public String getKey() {
      return key;
    }

    public LockAction getAction() {
      return action;
    }

    @Override
    public LockLevel getLevel() {
      return LockLevel.KEY;
    }
  }

  /**
   * multiple keys lock info.
   */
  public static class MultiKeyLockInfo implements LockInfoProvider {
    private final BucketLockInfo bucketLockInfo;
    private final List<String> keyList;
    private final LockAction action;

    public MultiKeyLockInfo(
        String bucketName, LockAction bucketAction, List<String> keyList, LockAction keyAction) {
      this.bucketLockInfo = new BucketLockInfo(bucketName, bucketAction);
      this.keyList = keyList;
      this.action = keyAction;
    }

    public BucketLockInfo getBucketLockInfo() {
      return bucketLockInfo;
    }

    public List<String> getKeyList() {
      return keyList;
    }

    public LockAction getAction() {
      return action;
    }

    @Override
    public LockLevel getLevel() {
      return LockLevel.MULTI_KEY;
    }
  }

  /**
   * way the lock should be taken.
   */
  public enum LockAction {
    NONE, READ, WRITE
  }

  /**
   * lock stage level, like volume, bucket, key.
   */
  public enum LockLevel {
    NONE,
    VOLUME,
    BUCKET,
    KEY,
    MULTI_KEY
  }
}

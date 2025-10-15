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

  private IOzoneManagerLock.ResourceManager resourceManager;

  LeveledResource(byte pos, String name) {
    // level of the resource
    this.mask = (short) (Math.pow(2, pos + 1) - 1);
    this.setMask = (short) Math.pow(2, pos);
    this.name = name;
    this.resourceManager = new IOzoneManagerLock.ResourceManager();
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
   *
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
  public IOzoneManagerLock.ResourceManager getResourceManager() {
    return resourceManager;
  }

  short getMask() {
    return mask;
  }
}

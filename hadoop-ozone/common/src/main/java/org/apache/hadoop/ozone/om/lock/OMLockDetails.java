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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMLockDetailsProto;

/**
 * This class is for recording detailed consumed time on Locks.
 */
public class OMLockDetails {

  public static final OMLockDetails EMPTY_DETAILS_LOCK_ACQUIRED =
      new OMLockDetails(true);
  public static final OMLockDetails EMPTY_DETAILS_LOCK_NOT_ACQUIRED =
      new OMLockDetails(false);
  private boolean lockAcquired;
  private long waitLockNanos;
  private long readLockNanos;
  private long writeLockNanos;

  public OMLockDetails() {
  }

  public OMLockDetails(boolean lockAcquired) {
    this.lockAcquired = lockAcquired;
  }

  enum LockOpType {
    WAIT,
    READ,
    WRITE
  }

  public void add(long timeNanos, LockOpType lockOpType) {
    switch (lockOpType) {
    case WAIT:
      waitLockNanos += timeNanos;
      break;
    case READ:
      readLockNanos += timeNanos;
      break;
    case WRITE:
      writeLockNanos += timeNanos;
      break;
    default:
    }
  }

  public void merge(OMLockDetails omLockDetails) {
    lockAcquired = omLockDetails.isLockAcquired();
    waitLockNanos += omLockDetails.getWaitLockNanos();
    readLockNanos += omLockDetails.getReadLockNanos();
    writeLockNanos += omLockDetails.getWriteLockNanos();
  }

  public long getWaitLockNanos() {
    return waitLockNanos;
  }

  public void setWaitLockNanos(long waitLockNanos) {
    this.waitLockNanos = waitLockNanos;
  }

  public long getReadLockNanos() {
    return readLockNanos;
  }

  public void setReadLockNanos(long readLockNanos) {
    this.readLockNanos = readLockNanos;
  }

  public long getWriteLockNanos() {
    return writeLockNanos;
  }

  public void setWriteLockNanos(long writeLockNanos) {
    this.writeLockNanos = writeLockNanos;
  }

  public boolean isLockAcquired() {
    return lockAcquired;
  }

  public void setLockAcquired(boolean lockAcquired) {
    this.lockAcquired = lockAcquired;
  }

  public OMLockDetailsProto.Builder toProtobufBuilder() {
    return OMLockDetailsProto.newBuilder()
        .setIsLockAcquired(isLockAcquired())
        .setWaitLockNanos(getWaitLockNanos())
        .setReadLockNanos(getReadLockNanos())
        .setWriteLockNanos(getWriteLockNanos());
  }

  @Override
  public String toString() {
    return "OMLockDetails{" +
        "lockAcquired=" + lockAcquired +
        ", waitLockNanos=" + waitLockNanos +
        ", readLockNanos=" + readLockNanos +
        ", writeLockNanos=" + writeLockNanos +
        '}';
  }

  public void clear() {
    lockAcquired = false;
    waitLockNanos = 0;
    readLockNanos = 0;
    writeLockNanos = 0;
  }
}

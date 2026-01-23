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

import static org.apache.hadoop.ozone.om.lock.OMLockDetails.EMPTY_DETAILS_LOCK_ACQUIRED;
import static org.apache.hadoop.ozone.om.lock.OMLockDetails.EMPTY_DETAILS_LOCK_NOT_ACQUIRED;

import java.util.Collection;

/**
 * Read only "lock" for snapshots
 * Uses no lock.  Always returns true when acquiring
 * read lock and false for write locks
 */
public class OmReadOnlyLock implements IOzoneManagerLock {

  @Override
  public OMLockDetails acquireReadLock(Resource resource, String... resources) {
    return EMPTY_DETAILS_LOCK_ACQUIRED;
  }

  @Override
  public OMLockDetails acquireReadLocks(Resource resource, Collection<String[]> resources) {
    return EMPTY_DETAILS_LOCK_ACQUIRED;
  }

  @Override
  public OMLockDetails acquireWriteLock(Resource resource,
      String... resources) {
    return EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
  }

  @Override
  public OMLockDetails acquireWriteLocks(Resource resource, Collection<String[]> resources) {
    return EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
  }

  @Override
  public OMLockDetails acquireResourceWriteLock(Resource resource) {
    return EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
  }

  @Override
  public boolean acquireMultiUserLock(String firstUser, String secondUser) {
    return false;
  }

  @Override
  public void releaseMultiUserLock(String firstUser, String secondUser) {
  // Intentionally empty
  }

  @Override
  public OMLockDetails releaseWriteLock(Resource resource,
      String... resources) {
    return EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
  }

  @Override
  public OMLockDetails releaseWriteLocks(Resource resource, Collection<String[]> resources) {
    return EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
  }

  @Override
  public OMLockDetails releaseResourceWriteLock(Resource resource) {
    return EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
  }

  @Override
  public OMLockDetails releaseReadLock(Resource resource, String... resources) {
    return EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
  }

  @Override
  public OMLockDetails releaseReadLocks(Resource resource, Collection<String[]> resources) {
    return EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
  }

  @Override
  public int getReadHoldCount(Resource resource, String... resources) {
    return 0;
  }

  @Override
  public int getWriteHoldCount(Resource resource, String... resources) {
    return 0;
  }

  @Override
  public boolean isWriteLockedByCurrentThread(Resource resource,
      String... resources) {
    return false;
  }

  @Override
  public void cleanup() {
  // Intentionally empty
  }

  @Override
  public OMLockMetrics getOMLockMetrics() {
    throw new UnsupportedOperationException(
        "OmReadOnlyLock does not support this operation.");
  }
}

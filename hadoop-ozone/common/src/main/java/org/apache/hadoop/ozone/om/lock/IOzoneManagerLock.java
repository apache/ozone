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

import com.google.common.annotations.VisibleForTesting;

/**
 * Interface for OM Metadata locks.
 */
public interface IOzoneManagerLock {
  @Deprecated
  boolean acquireLock(OzoneManagerLock.Resource resource, String... resources);

  boolean acquireReadLock(OzoneManagerLock.Resource resource,
                          String... resources);

  boolean acquireReadHashedLock(OzoneManagerLock.Resource resource,
                           String resourceName);

  boolean acquireWriteLock(OzoneManagerLock.Resource resource,
                           String... resources);

  boolean acquireWriteHashedLock(OzoneManagerLock.Resource resource,
                                 String resourceName);

  String generateResourceName(OzoneManagerLock.Resource resource,
                        String... resources);

  boolean acquireMultiUserLock(String firstUser, String secondUser);

  void releaseMultiUserLock(String firstUser, String secondUser);

  void releaseWriteLock(OzoneManagerLock.Resource resource,
                        String... resources);

  void releaseWriteHashedLock(OzoneManagerLock.Resource resource,
                        String resourceName);

  void releaseReadLock(OzoneManagerLock.Resource resource, String... resources);

  void releaseReadHashedLock(OzoneManagerLock.Resource resource,
                        String resourceName);

  @Deprecated
  void releaseLock(OzoneManagerLock.Resource resource, String... resources);

  @VisibleForTesting
  int getReadHoldCount(String resourceName);

  @VisibleForTesting
  String getReadLockWaitingTimeMsStat();

  @VisibleForTesting
  long getLongestReadLockWaitingTimeMs();

  @VisibleForTesting
  String getReadLockHeldTimeMsStat();

  @VisibleForTesting
  long getLongestReadLockHeldTimeMs();

  @VisibleForTesting
  int getWriteHoldCount(String resourceName);

  @VisibleForTesting
  boolean isWriteLockedByCurrentThread(String resourceName);

  @VisibleForTesting
  String getWriteLockWaitingTimeMsStat();

  @VisibleForTesting
  long getLongestWriteLockWaitingTimeMs();

  @VisibleForTesting
  String getWriteLockHeldTimeMsStat();

  @VisibleForTesting
  long getLongestWriteLockHeldTimeMs();

  void cleanup();

  OMLockMetrics getOMLockMetrics();
}

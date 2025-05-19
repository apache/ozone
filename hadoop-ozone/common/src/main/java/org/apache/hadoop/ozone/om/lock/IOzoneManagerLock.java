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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;

/**
 * Interface for OM Metadata locks.
 */
public interface IOzoneManagerLock {

  OMLockDetails acquireReadLock(OzoneManagerLock.LeveledResource resource,
                                String... resources);

  OMLockDetails acquireReadLocks(OzoneManagerLock.LeveledResource resource, Collection<String[]> resources);

  OMLockDetails acquireWriteLock(OzoneManagerLock.LeveledResource resource,
                                 String... resources);

  OMLockDetails acquireWriteLocks(OzoneManagerLock.LeveledResource resource,
                                 Collection<String[]> resources);

  boolean acquireMultiUserLock(String firstUser, String secondUser);

  void releaseMultiUserLock(String firstUser, String secondUser);

  OMLockDetails releaseWriteLock(OzoneManagerLock.LeveledResource resource,
                        String... resources);

  OMLockDetails releaseWriteLocks(OzoneManagerLock.LeveledResource resource,
                                 Collection<String[]> resources);

  OMLockDetails releaseReadLock(OzoneManagerLock.LeveledResource resource,
                                String... resources);

  OMLockDetails releaseReadLocks(OzoneManagerLock.LeveledResource resource,
                                Collection<String[]> resources);

  @VisibleForTesting
  int getReadHoldCount(OzoneManagerLock.LeveledResource resource,
      String... resources);

  @VisibleForTesting
  int getWriteHoldCount(OzoneManagerLock.LeveledResource resource,
      String... resources);

  @VisibleForTesting
  boolean isWriteLockedByCurrentThread(OzoneManagerLock.LeveledResource resource,
      String... resources);

  void cleanup();

  OMLockMetrics getOMLockMetrics();
}

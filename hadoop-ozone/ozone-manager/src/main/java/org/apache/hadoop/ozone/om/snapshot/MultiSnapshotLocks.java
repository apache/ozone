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

package org.apache.hadoop.ozone.om.snapshot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock;

/**
 * Class to take multiple locks on multiple snapshots.
 */
public class MultiSnapshotLocks {
  private final List<String[]> objectLocks;
  private final IOzoneManagerLock lock;
  private final OzoneManagerLock.Resource resource;
  private final boolean writeLock;
  private OMLockDetails lockDetails;

  public MultiSnapshotLocks(IOzoneManagerLock lock, OzoneManagerLock.Resource resource, boolean writeLock) {
    this.writeLock = writeLock;
    this.resource = resource;
    this.lock = lock;
    this.objectLocks = new ArrayList<>();
    this.lockDetails = OMLockDetails.EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
  }

  public synchronized OMLockDetails acquireLock(Collection<UUID> ids) throws OMException {
    if (this.lockDetails.isLockAcquired()) {
      throw new OMException(
          objectLocks.stream().map(Arrays::toString).collect(Collectors.joining(",",
              "More locks cannot be acquired when locks have been already acquired. Locks acquired : [", "]")),
          OMException.ResultCodes.INTERNAL_ERROR);
    }
    List<String[]> keys =
        ids.stream().filter(Objects::nonNull).map(id -> new String[] {id.toString()})
            .collect(Collectors.toList());
    OMLockDetails omLockDetails = this.writeLock ? lock.acquireWriteLocks(resource, keys) :
        lock.acquireReadLocks(resource, keys);
    if (omLockDetails.isLockAcquired()) {
      objectLocks.addAll(keys);
    }
    this.lockDetails = omLockDetails;
    return omLockDetails;
  }

  public synchronized void releaseLock() {
    if (this.writeLock) {
      lockDetails = lock.releaseWriteLocks(resource, this.objectLocks);
    } else {
      lockDetails = lock.releaseReadLocks(resource, this.objectLocks);
    }
    this.objectLocks.clear();
  }

  List<String[]> getObjectLocks() {
    return objectLocks;
  }

  public boolean isLockAcquired() {
    return lockDetails.isLockAcquired();
  }
}

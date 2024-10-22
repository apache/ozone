/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.lock;

import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Class to take multiple locks on a resource.
 */
public class MultiLocks<T> {
  private final Queue<T> objectLocks;
  private final IOzoneManagerLock lock;
  private final OzoneManagerLock.Resource resource;
  private final boolean writeLock;

  public MultiLocks(IOzoneManagerLock lock, OzoneManagerLock.Resource resource, boolean writeLock) {
    this.writeLock = writeLock;
    this.resource = resource;
    this.lock = lock;
    this.objectLocks = new LinkedList<>();
  }

  public OMLockDetails acquireLock(Collection<T> objects) throws OMException {
    if (!objectLocks.isEmpty()) {
      throw new OMException("More locks cannot be acquired when locks have been already acquired. Locks acquired : "
          + objectLocks, OMException.ResultCodes.INTERNAL_ERROR);
    }
    OMLockDetails omLockDetails = OMLockDetails.EMPTY_DETAILS_LOCK_ACQUIRED;
    for (T object : objects) {
      if (object != null) {
        omLockDetails = this.writeLock ? lock.acquireWriteLock(resource, object.toString())
            : lock.acquireReadLock(resource, object.toString());
        objectLocks.add(object);
        if (!omLockDetails.isLockAcquired()) {
          break;
        }
      }
    }
    if (!omLockDetails.isLockAcquired()) {
      releaseLock();
    }
    return omLockDetails;
  }

  public void releaseLock() {
    while (!objectLocks.isEmpty()) {
      T object = objectLocks.poll();
      if (this.writeLock) {
        lock.releaseWriteLock(resource, object.toString());
      } else {
        lock.releaseReadLock(resource, object.toString());
      }
    }
  }
}

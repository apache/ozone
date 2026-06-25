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

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * The LeveledResourceLockTracker class is a singleton that extends the
 * {@link ResourceLockTracker} to manage locks on leveled resources defined
 * by {@link OzoneManagerLock.LeveledResource}. This class provides functionality
 * to handle lock operations for resources that are managed at different hierarchical
 * levels, utilizing a {@link ThreadLocal} mechanism for tracking the current lock set
 * for each thread.
 *
 * The class ensures thread-safety by restricting lock access to the current thread's
 * context. It allows checking if a resource can be locked, locking a resource, unlocking
 * a resource, and retrieving details of currently locked resources.
 *
 * Thread safety is achieved through the use of a volatile singleton instance and
 * double-checked locking during initialization. The internal lock state is maintained
 * per-thread using a {@link ThreadLocal} variable.
 *
 * Key methods include:
 *  - {@code get()}: Retrieves the singleton instance of the LeveledResourceLockTracker.
 *  - {@code canLockResource(OzoneManagerLock.LeveledResource resource)}: Determines
 *    if a specific resource can be locked based on the current lock set.
 *  - {@code getCurrentLockedResources()}: Provides a stream of currently locked
 *    resources for the current thread.
 *  - {@code lockResource(OzoneManagerLock.LeveledResource resource)}: Locks a specified
 *    resource and updates the current thread's lock state.
 *  - {@code unlockResource(OzoneManagerLock.LeveledResource resource)}: Unlocks a specified
 *    resource and updates the current thread's lock state accordingly.
 *
 * This class is final to prevent subclassing, ensuring the integrity of its
 * singleton behavior and thread-local lock tracking mechanism.
 */
final class LeveledResourceLockTracker extends ResourceLockTracker<OzoneManagerLock.LeveledResource> {
  private final ThreadLocal<Short> lockSet = ThreadLocal.withInitial(() -> Short.valueOf((short) 0));
  private static volatile LeveledResourceLockTracker instance = null;

  private LeveledResourceLockTracker() {
  }

  public static LeveledResourceLockTracker get() {
    if (instance == null) {
      synchronized (LeveledResourceLockTracker.class) {
        if (instance == null) {
          instance = new LeveledResourceLockTracker();
        }
      }
    }
    return instance;
  }

  @Override
  public boolean canLockResource(OzoneManagerLock.LeveledResource resource) {
    return resource.canLock(lockSet.get());
  }

  @Override
  Stream<OzoneManagerLock.LeveledResource> getCurrentLockedResources() {
    short lockSetVal = lockSet.get();
    return Arrays.stream(OzoneManagerLock.LeveledResource.values())
        .filter(leveledResource -> leveledResource.isLevelLocked(lockSetVal));
  }

  @Override
  public OMLockDetails unlockResource(OzoneManagerLock.LeveledResource resource) {
    lockSet.set(resource.clearLock(lockSet.get()));
    return super.unlockResource(resource);
  }

  @Override
  public OMLockDetails lockResource(OzoneManagerLock.LeveledResource resource) {
    lockSet.set(resource.setLock(lockSet.get()));
    return super.lockResource(resource);
  }
}

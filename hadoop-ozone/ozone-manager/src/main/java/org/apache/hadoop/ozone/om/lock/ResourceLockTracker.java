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

import java.util.stream.Stream;

/**
 * Abstract class that tracks resource locks based on a generic resource type.
 * Provides methods to acquire, release, and inspect locks, as well as obtain
 * details about the locks held by the current thread.
 *
 * @param <T> the type of resource being tracked, which must implement
 *            {@link IOzoneManagerLock.Resource}.
 */
abstract class ResourceLockTracker<T extends IOzoneManagerLock.Resource> {

  private final ThreadLocal<OMLockDetails> omLockDetails = ThreadLocal.withInitial(OMLockDetails::new);

  abstract boolean canLockResource(T resource);

  abstract Stream<T> getCurrentLockedResources();

  OMLockDetails clearLockDetails() {
    omLockDetails.get().clear();
    return getOmLockDetails();
  }

  OMLockDetails unlockResource(T resource) {
    return getOmLockDetails();
  }

  OMLockDetails lockResource(T resource) {
    omLockDetails.get().setLockAcquired(true);
    return getOmLockDetails();
  }

  public OMLockDetails getOmLockDetails() {
    return omLockDetails.get();
  }
}

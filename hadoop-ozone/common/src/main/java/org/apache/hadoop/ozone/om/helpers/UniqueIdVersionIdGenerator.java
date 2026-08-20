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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.utils.UniqueId;

/**
 * Numbers a version with the time it was written, through the scheme Ozone
 * already uses for block local IDs: {@code currentTimeMillis << 16} with a
 * 16-bit counter separating ids proposed inside one millisecond.
 *
 * <p>Needs no allocator state and no coordination. It does not promise the
 * per-key ordering the versionedKeyTable needs — the counter can run out, and a
 * clock can step backwards — which {@code VersionIdAllocator} establishes by
 * taking a proposal as a floor.
 */
public class UniqueIdVersionIdGenerator implements VersionIdGenerator {

  @Override
  public long generateVersionId() {
    return UniqueId.next();
  }
}

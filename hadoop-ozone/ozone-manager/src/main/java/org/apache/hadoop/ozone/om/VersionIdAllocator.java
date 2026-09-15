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

package org.apache.hadoop.ozone.om;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.VersionIdGenerator;

/**
 * Settles the versionId of a version as it is applied, from the id proposed for
 * it before the request was replicated.
 *
 * <p>The versionedKeyTable orders a key's versions by
 * {@code Long.MAX_VALUE - versionId}, so the ids of one key have to increase in
 * the order the versions were written. A proposal is a clock reading and cannot
 * promise that: ids proposed inside one millisecond can exhaust the counter
 * separating them, and a leader change onto a lagging clock proposes a lower
 * value.
 *
 * <p>So a proposal is a floor. The applied id is the later of it and the id
 * after the key's current version, which the write path already holds — no read
 * of its own, no global state, and identical on every OM. Under a clock
 * regression an affected key's ids climb by one until proposals overtake them
 * again: the versions stay ordered and only the id's reading as a time
 * degrades.
 */
public class VersionIdAllocator {

  private final VersionIdGenerator generator;

  public VersionIdAllocator(ConfigurationSource conf) {
    this(VersionIdGenerator.fromConfiguration(conf));
  }

  public VersionIdAllocator(VersionIdGenerator generator) {
    this.generator = generator;
  }

  public VersionIdGenerator getGenerator() {
    return generator;
  }

  /** Proposes an id, on the OM that received the request. */
  public long propose() {
    final long versionId = generator.generateVersionId();
    Preconditions.checkState(versionId > VersionIdGenerator.UNSET_VERSION_ID,
        "%s proposed the reserved versionId %s",
        generator.getClass().getName(), versionId);
    return versionId;
  }

  /**
   * @param proposedVersionId the id the request carried, which must be set:
   *     an unset field would otherwise be applied as the reserved unset value
   * @param currentVersion the key's current version, or null when it has none.
   *     The write path holds it already, so no extra read is needed here.
   * @return the versionId to freeze on the version being applied
   */
  public long allocate(long proposedVersionId, OmKeyInfo currentVersion) {
    Preconditions.checkArgument(
        proposedVersionId > VersionIdGenerator.UNSET_VERSION_ID,
        "no versionId was proposed for this version; every request that can "
            + "create one has to propose it in preExecute");

    final long versionId =
        generator.versionIdFor(proposedVersionId, currentVersion != null);

    if (currentVersion == null) {
      return versionId;
    }

    final Long currentVersionId = currentVersion.getVersionId();
    if (currentVersionId == null) {
      // Written before versioning was enabled, so it carries no id and the key
      // has no other versions to order against.
      return versionId;
    }

    return Math.max(versionId, currentVersionId + 1);
  }
}

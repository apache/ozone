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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Assigns the versionId of an object version when the version is committed.
 *
 * <p>Deployments differ in whether they need a version identity that can be
 * constructed without listing, so the implementation is chosen per cluster
 * through {@link OMConfigKeys#OZONE_OM_VERSIONING_VERSION_ID_GENERATOR}.
 * Implementations must be public, have a public no-argument constructor, and
 * satisfy the constraints that the versionedKeyTable layout and version
 * promotion rely on:
 *
 * <ul>
 *   <li>ids increase within a key: a version created later has a larger id;</li>
 *   <li>an id is assigned once when the version is created and never changes
 *       afterwards, so that external references stay valid;</li>
 *   <li>{@link #NULL_VERSION_ID} is reserved and is never generated.</li>
 * </ul>
 *
 * <p>The generator is cluster-wide and may be changed on a running cluster, so
 * these constraints hold per generator but not necessarily across a change of
 * generator. Colliding ids are rejected at commit time rather than prevented
 * here; see {@code VersionIdAllocator}.
 */
public interface VersionIdGenerator {

  /**
   * Reserved id of the null version slot, rendered as the literal "null" by
   * the S3 layer. Never returned by a generator.
   */
  long NULL_VERSION_ID = 0L;

  /**
   * Reserved id of the pinned first version of a key, a separate slot from
   * {@link #NULL_VERSION_ID}. Only assigned by generators that pin the first
   * version of a key; it is smaller than any transaction index, so such a
   * version sorts at the old end of the key's version sequence.
   */
  long FIRST_VERSION_ID = 1L;

  /**
   * Generates the versionId to freeze on a version being committed.
   *
   * @param transactionLogIndex index of the committing OM Ratis transaction
   * @param hasCurrentVersion whether keyTable already holds a current version
   *     of the key being committed. The write path looks the current version up
   *     anyway, so generators that treat the first version of a key specially
   *     need no read of their own.
   * @return the versionId of the new version
   */
  long generateVersionId(long transactionLogIndex, boolean hasCurrentVersion);

  /**
   * Instantiates the generator configured for this cluster.
   *
   * @throws RuntimeException if the configured class cannot be instantiated
   */
  static VersionIdGenerator fromConfiguration(ConfigurationSource conf) {
    Class<? extends VersionIdGenerator> generatorClass = conf.getClass(
        OMConfigKeys.OZONE_OM_VERSIONING_VERSION_ID_GENERATOR,
        OMConfigKeys.OZONE_OM_VERSIONING_VERSION_ID_GENERATOR_DEFAULT,
        VersionIdGenerator.class);
    return ReflectionUtils.newInstance(generatorClass, null);
  }
}

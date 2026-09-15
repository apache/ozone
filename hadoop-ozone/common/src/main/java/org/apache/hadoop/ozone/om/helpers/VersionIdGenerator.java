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
 * Numbers an object version.
 *
 * <p>The id is proposed on the OM that received the request, in preExecute, so
 * it travels in the replicated request and every OM applies a version that is
 * already numbered. Nothing about it depends on the transaction carrying the
 * write, or on OM being replicated by Ratis.
 *
 * <p>Implementations must be public with a public no-argument constructor, and
 * are selected cluster-wide by
 * {@link OMConfigKeys#OZONE_OM_VERSIONING_VERSION_ID_GENERATOR}.
 */
public interface VersionIdGenerator {

  /**
   * Unset value of the optional versionId field, carried by records written
   * before versioning existed. Never proposed.
   *
   * <p>Not the id of the null version, which carries a proposed id like any
   * other version and is marked by {@code isNullVersion} instead.
   */
  long UNSET_VERSION_ID = 0L;

  /** @return the id to propose for a version being created. */
  long generateVersionId();

  /**
   * The id a version takes when applied, given what was proposed for it. A
   * generator that numbers some versions specially decides that here, where
   * whether the key already has a version is settled under the write's lock.
   *
   * @param proposedVersionId the id the request carried
   * @param hasCurrentVersion whether keyTable already holds a version of the key
   */
  default long versionIdFor(long proposedVersionId, boolean hasCurrentVersion) {
    return proposedVersionId;
  }

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

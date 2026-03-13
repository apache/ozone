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

package org.apache.hadoop.hdds;

import java.util.Optional;
import org.apache.hadoop.ozone.upgrade.UpgradeAction;

/**
 * The logical versioning system used to track incompatible changes to a component, regardless whether they affect disk
 * or network compatibility between the same or different types of components.
 *
 * This interface is the base type for component version enums.
 */
public interface ComponentVersion {
  /**
   * Returns an integer representation of this version. To callers outside this class, this is an opaque value which
   * should not be checked or compared directly. {@link #isSupportedBy} should be used for version comparisons.
   *
   * To implementors of this interface, versions should serialize such that version1 <= version2
   * if and only if version1.serialize() <= version2.serialize().
   * Negative numbers may be used as serialized values to represent unknown future versions which are trivially larger
   * than all other versions.
   *
   * @return The serialized representation of this version.
   */
  int serialize();

  /**
   * @return The description of the version enum value.
   */
  String description();

  /**
   * @return The next version immediately following this one, or null if there is no such version.
   */
  ComponentVersion nextVersion();

  /**
   * Uses the serialized representation of a ComponentVersion to check if its feature set is supported by the current
   * ComponentVersion.
   *
   * @return true if this version supports the features of the provided version. False otherwise.
   */
  default boolean isSupportedBy(int serializedVersion) {
    if (serialize() < 0) {
      // Our version is an unknown future version, it is not supported by any other version.
      return false;
    } else if (serializedVersion < 0) {
      // The other version is an unknown future version, it trivially supports all other versions.
      return true;
    } else {
      // If both versions have positive values, they represent concrete versions and we can compare them directly.
      return serialize() <= serializedVersion;
    }
  }

  /**
   * @return true if this version supports the features of the provided version. False otherwise.
   */
  default boolean isSupportedBy(ComponentVersion other) {
    return isSupportedBy(other.serialize());
  }

  default Optional<? extends UpgradeAction> action() {
    return Optional.empty();
  }
}

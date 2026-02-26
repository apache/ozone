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
 * Base type for component version enums.
 */
public interface ComponentVersion {
  /**
   * @return The serialized representation of this version. This is an opaque value which should not be checked or
   * compared directly.
   */
  int serialize();

  /**
   * @return the description of the version enum value.
   */
  String description();

  /**
   * Deserializes a ComponentVersion and checks if its feature set is supported by the current ComponentVersion.
   *
   * @return true if this version supports the features of otherVersion. False otherwise.
   */
  boolean isSupportedBy(int serializedVersion);

  default Optional<? extends UpgradeAction> action() {
    return Optional.empty();
  }
}

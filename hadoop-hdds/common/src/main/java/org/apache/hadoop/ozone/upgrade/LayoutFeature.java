/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.upgrade;

import java.util.Optional;

/**
 * Generic Layout feature interface for Ozone.
 */
public interface LayoutFeature {
  String name();

  int layoutVersion();

  String description();

  default Optional<? extends UpgradeAction> action(UpgradeActionType p) {
    return Optional.empty();
  }

  /**
   * Generic UpgradeAction interface. An operation that is run on specific
   * upgrade states like post finalize, pre-downgrade etc.
   * @param <T>
   */
  interface UpgradeAction<T> {

    default String name() {
      return getClass().getSimpleName();
    }

    void execute(T arg) throws Exception;
  }

  /**
   * Phase of execution for this action.
   */
  enum UpgradeActionType {

    // Run every time an unfinalized component is started up.
    UNFINALIZED_STATE_VALIDATION,

    // Run exactly once when an upgraded cluster is detected with this new
    // layout version.
    // NOTE 1 : This will not be run in a NEW cluster!
    // NOTE 2 : This needs to be a backward compatible action until a DOWNGRADE
    // hook is provided!
    // NOTE 3 : These actions are not submitted through RATIS (TODO)
    ON_FIRST_UPGRADE_START,

    // Run exactly once during finalization of layout feature.
    ON_FINALIZE
  }
}

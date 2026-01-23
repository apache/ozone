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

package org.apache.hadoop.ozone.upgrade;

import java.util.Optional;
import org.apache.hadoop.ozone.Versioned;

/**
 * Generic Layout feature interface for Ozone.
 */
public interface LayoutFeature extends Versioned {
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

  @Override
  default int version() {
    return this.layoutVersion();
  }

  /**
   * Phase of execution for this action.
   */
  enum UpgradeActionType {

    // Run every time an un-finalized component is started up.
    VALIDATE_IN_PREFINALIZE,

    // Run once when an upgraded cluster is started with this new
    // layout version.
    // If the action fails, it will be run again when the component is
    // restarted.
    // If updating the VERSION file fails, the action may be run again when the
    // component is restarted, even if it finished successfully.
    // NOTE 1 : This will not be run in a NEW cluster!
    // NOTE 2 : This needs to be a backward compatible action until a DOWNGRADE
    //  hook is provided!
    //  Even if the action fails partway through, all on disk structures should
    //  still be in a backwards compatible state.
    // NOTE 3 : These actions are not submitted through RATIS (TODO)
    ON_FIRST_UPGRADE_START,

    // Run once during finalization of the layout feature.
    // If the action fails, it will be run again when finalization is retried.
    // If updating the VERSION file fails, the action may be run again when
    // finalization is retried, even if it finished successfully.
    ON_FINALIZE
  }
}

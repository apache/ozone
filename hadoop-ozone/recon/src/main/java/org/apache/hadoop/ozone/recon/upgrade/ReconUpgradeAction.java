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

package org.apache.hadoop.ozone.recon.upgrade;

import javax.sql.DataSource;

/**
 * ReconUpgradeAction is an interface for executing upgrade actions in Recon.
 */
public interface ReconUpgradeAction {

  /**
   * Defines the different phases during which upgrade actions can be executed.
   * Each action type corresponds to a specific point in the upgrade process:
   *
   * - FINALIZE: This action is executed automatically during the startup
   *   of Recon when it finalizes the layout upgrade. It ensures that all necessary
   *   upgrades or schema changes are applied to bring the system in sync with
   *   the latest version.
   */
  enum UpgradeActionType {
    FINALIZE
  }

  /**
   * Execute the upgrade action.
   */
  void execute(DataSource source) throws Exception;

  /**
   * Provides the type of upgrade phase (e.g., FINALIZE).
   */
  UpgradeActionType getType();
}

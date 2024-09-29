/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.recon.upgrade;

/**
 * Enum representing Recon layout features with their version, description,
 * and associated upgrade action to be executed during an upgrade.
 */
public enum ReconLayoutFeature {
  FEATURE_1(1, "Description for Feature 1", new Feature1UpgradeAction()),
  FEATURE_2(2, "Description for Feature 2", new Feature2UpgradeAction()),
  FEATURE_3(3, "Description for Feature 3", new Feature3UpgradeAction());

  private final int version;
  private final String description;
  private final ReconUpgradeAction upgradeAction;

  ReconLayoutFeature(int version, String description, ReconUpgradeAction upgradeAction) {
    this.version = version;
    this.description = description;
    this.upgradeAction = upgradeAction;
  }

  public int getVersion() {
    return version;
  }

  public String getDescription() {
    return description;
  }

  public ReconUpgradeAction getUpgradeAction() {
    return upgradeAction;
  }
}

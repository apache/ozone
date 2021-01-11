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

package org.apache.hadoop.ozone.container.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeatureCatalog.HDDSLayoutFeature.FIRST_UPGRADE_VERSION;

import java.util.Optional;

import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeatureCatalog.HDDSLayoutFeature;

/**
 * Catalog of HDDS features and their corresponding DataNode action.
 * It is OK to skip HDDS features from the catalog that do not have
 * any specific DataNodeActions.
 */
public class DataNodeLayoutActionCatalog {

  /**
   * List of HDDS Features and corresponding DataNode actions.
   */
  public enum DataNodeLayoutAction {
    DataNodeAction1(FIRST_UPGRADE_VERSION,
        new DataNodeUpgradeActionFirstUpgradeVersion());

    //////////////////////////////  //////////////////////////////

    private HDDSLayoutFeature hddsFeature;
    private DataNodeUpgradeAction dataNodeAction;

    DataNodeLayoutAction(HDDSLayoutFeature feature,
                         DataNodeUpgradeAction action) {
      this.hddsFeature = feature;
      this.dataNodeAction = action;
      this.hddsFeature.setDataNodeUpgradeAction(Optional.of(dataNodeAction));
    }

    public HDDSLayoutFeature getHddsFeature() {
      return hddsFeature;
    }
  }
}


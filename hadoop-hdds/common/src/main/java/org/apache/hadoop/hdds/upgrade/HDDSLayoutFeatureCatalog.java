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

package org.apache.hadoop.hdds.upgrade;

import java.util.Optional;

import org.apache.hadoop.ozone.upgrade.LayoutFeature;

/**
 * Catalog of HDDS features.
 */
public class HDDSLayoutFeatureCatalog {

  /**
   * List of HDDS Features.
   */
  public enum HDDSLayoutFeature implements LayoutFeature {
    INITIAL_VERSION(0, "Initial Layout Version");


    private int layoutVersion;
    private String description;
    private Optional<HDDSUpgradeAction> hddsUpgradeAction = Optional.empty();

    HDDSLayoutFeature(final int layoutVersion, String description) {
      this.layoutVersion = layoutVersion;
      this.description = description;
    }

    HDDSLayoutFeature(final int layoutVersion, String description,
                    HDDSUpgradeAction upgradeAction) {
      this.layoutVersion = layoutVersion;
      this.description = description;
      hddsUpgradeAction = Optional.of(upgradeAction);
    }

    @Override
    public int layoutVersion() {
      return layoutVersion;
    }

    @Override
    public String description() {
      return description;
    }

    @Override
    public Optional<? extends HDDSUpgradeAction> onFinalizeAction() {
      return hddsUpgradeAction;
    }
  }
}


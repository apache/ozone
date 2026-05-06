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

package org.apache.hadoop.hdds.upgrade;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.hadoop.ozone.upgrade.AbstractLayoutVersionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to manage layout versions and features for Storage Container Manager
 * and DataNodes.
 */
@SuppressWarnings("FinalClass")
public class HDDSLayoutVersionManager extends
    AbstractLayoutVersionManager<HDDSLayoutFeature> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HDDSLayoutVersionManager.class);
  private static final String[] HDDS_CLASS_UPGRADE_PACKAGES = new String[] {
      "org.apache.hadoop.hdds.scm.server",
      "org.apache.hadoop.ozone.container",
  };

  public HDDSLayoutVersionManager(int layoutVersion) throws IOException {
    init(layoutVersion, HDDSLayoutFeature.values());
    registerUpgradeActions(HDDS_CLASS_UPGRADE_PACKAGES);
  }

  public static int maxLayoutVersion() {
    HDDSLayoutFeature[] features = HDDSLayoutFeature.values();
    return features[features.length - 1].layoutVersion();
  }

  @VisibleForTesting
  void registerUpgradeActions(String... packages) {
    ScmUpgradeActionProvider scmProvider = new ScmUpgradeActionProvider(packages);
    scmProvider.load().forEach((feature, action) -> {
      HDDSLayoutFeature hddsFeature = (HDDSLayoutFeature) feature;
      if (hddsFeature.layoutVersion() > getMetadataLayoutVersion()) {
        hddsFeature.addScmAction(action);
      } else {
        LOG.debug("Skipping SCM Upgrade Action {} since it has been finalized.", action.name());
      }
    });

    DatanodeUpgradeActionProvider dnProvider = new DatanodeUpgradeActionProvider(packages);
    dnProvider.load().forEach((feature, action) -> {
      HDDSLayoutFeature hddsFeature = (HDDSLayoutFeature) feature;
      if (hddsFeature.layoutVersion() > getMetadataLayoutVersion()) {
        hddsFeature.addDatanodeAction(action);
      } else {
        LOG.debug("Skipping Datanode Upgrade Action {} since it has been finalized.", action.name());
      }
    });
  }
}

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


import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.SCM;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.ozone.upgrade.AbstractLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class to manage layout versions and features for Storage Container Manager
 * and DataNodes.
 */
@SuppressWarnings("FinalClass")
public class HDDSLayoutVersionManager extends
    AbstractLayoutVersionManager<HDDSLayoutFeature> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HDDSLayoutVersionManager.class);
  private static final String[] HDDS_CLASS_UPGRADE_PACKAGES = new String[]{
      "org.apache.hadoop.hdds.scm.server",
      "org.apache.hadoop.ozone.container"};

  public HDDSLayoutVersionManager(int layoutVersion) throws IOException {
    init(layoutVersion, HDDSLayoutFeature.values());
    registerUpgradeActions(HDDS_CLASS_UPGRADE_PACKAGES);
  }

  public static int maxLayoutVersion() {
    HDDSLayoutFeature[] features = HDDSLayoutFeature.values();
    return features[features.length - 1].layoutVersion();
  }

  /**
   * Scan classpath and register all actions to layout features.
   */
  @VisibleForTesting
  void registerUpgradeActions(String[] packageNames) {
    Reflections reflections = new Reflections(packageNames);
    Set<Class<?>> typesAnnotatedWith =
        reflections.getTypesAnnotatedWith(UpgradeActionHdds.class);
    typesAnnotatedWith.forEach(actionClass -> {
      if (HDDSUpgradeAction.class.isAssignableFrom(actionClass)) {
        try {
          HDDSUpgradeAction action =
              (HDDSUpgradeAction) actionClass.newInstance();
          UpgradeActionHdds annotation =
              actionClass.getAnnotation(UpgradeActionHdds.class);
          HDDSLayoutFeature feature = annotation.feature();
          if (feature.layoutVersion() > getMetadataLayoutVersion()) {
            LOG.info("Registering Upgrade Action : {}", action.name());
            if (annotation.component() == SCM) {
              feature.addScmAction(annotation.type(), action);
            } else {
              feature.addDatanodeAction(annotation.type(), action);
            }
          } else {
            LOG.debug("Skipping Upgrade Action {} since it has been finalized" +
                ".", action.name());
          }
        } catch (Exception e) {
          LOG.error("Cannot instantiate Upgrade Action class {}",
              actionClass.getSimpleName(), e);
        }
      } else {
        LOG.warn("Found upgrade action class not of type " +
                "org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction : {}",
            actionClass.getName());
      }
    });
  }
}

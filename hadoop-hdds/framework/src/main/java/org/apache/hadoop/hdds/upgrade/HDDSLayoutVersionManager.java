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

import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.SCM;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.ozone.upgrade.AbstractLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;
import org.reflections.Reflections;
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
  private static final Object[] HDDS_CLASS_UPGRADE_PACKAGES = new Object[] {
      "org.apache.hadoop.hdds.scm.server",
      "org.apache.hadoop.ozone.container",
  };

  private static final Reflections HDDS_UPGRADE_REFLECTIONS =
      new Reflections(HDDS_CLASS_UPGRADE_PACKAGES);

  public HDDSLayoutVersionManager(int layoutVersion) throws IOException {
    init(layoutVersion, HDDSLayoutFeature.values());
    registerUpgradeActions(getAnnotatedTypes(HDDS_UPGRADE_REFLECTIONS));
  }

  public static int maxLayoutVersion() {
    HDDSLayoutFeature[] features = HDDSLayoutFeature.values();
    return features[features.length - 1].layoutVersion();
  }

  @VisibleForTesting
  void registerUpgradeActions(Object... classNames) {
    registerUpgradeActions(getAnnotatedTypes(new Reflections(classNames)));
  }

  private static Set<Class<?>> getAnnotatedTypes(Reflections reflections) {
    return reflections.getTypesAnnotatedWith(UpgradeActionHdds.class);
  }

  /**
   * Scan classpath and register all actions to layout features.
   */
  private void registerUpgradeActions(Set<Class<?>> typesAnnotatedWith) {
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
              feature.addScmAction(action);
            } else {
              feature.addDatanodeAction(action);
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

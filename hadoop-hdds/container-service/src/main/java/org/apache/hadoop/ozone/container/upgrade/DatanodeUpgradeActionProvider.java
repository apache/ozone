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

package org.apache.hadoop.ozone.container.upgrade;

import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.DATANODE;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads {@link HDDSUpgradeAction} implementations annotated with
 * {@link UpgradeActionHdds} for datanode upgrades.
 */
public final class DatanodeUpgradeActionProvider
    implements ComponentUpgradeActionProvider<HDDSUpgradeAction<?>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeUpgradeActionProvider.class);

  /**
   * Package scanned for {@link UpgradeActionHdds}-annotated classes.
   */
  public static final String DATANODE_UPGRADE_CLASS_PACKAGE =
      "org.apache.hadoop.ozone.container";

  @Override
  public Map<ComponentVersion, HDDSUpgradeAction<?>> load() {
    Map<ComponentVersion, HDDSUpgradeAction<?>> upgradeActions = new HashMap<>();

    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .forPackages(DATANODE_UPGRADE_CLASS_PACKAGE)
        .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner())
        .setExpandSuperTypes(false)
        .setParallel(true));
    Set<Class<?>> typesAnnotatedWith =
        reflections.getTypesAnnotatedWith(UpgradeActionHdds.class);

    typesAnnotatedWith.forEach(actionClass -> {
      if (HDDSUpgradeAction.class.isAssignableFrom(actionClass)) {
        try {
          UpgradeActionHdds annotation =
              actionClass.getAnnotation(UpgradeActionHdds.class);
          if (annotation.component() != DATANODE) {
            return;
          }
          HDDSUpgradeAction<?> action =
              (HDDSUpgradeAction<?>) actionClass.getDeclaredConstructor().newInstance();
          HDDSLayoutFeature feature = annotation.feature();
          LOG.info("Registering Upgrade Action : {}", action.name());
          upgradeActions.put(feature, action);
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

    return upgradeActions;
  }
}

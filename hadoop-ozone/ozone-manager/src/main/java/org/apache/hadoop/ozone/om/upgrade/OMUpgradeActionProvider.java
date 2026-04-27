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

package org.apache.hadoop.ozone.om.upgrade;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads {@link OmUpgradeAction} implementations annotated with {@link UpgradeActionOm} from
 * {@link #OM_UPGRADE_CLASS_PACKAGE} only.
 */
public final class OMUpgradeActionProvider implements ComponentUpgradeActionProvider<OmUpgradeAction> {

  private static final Logger LOG = LoggerFactory.getLogger(OMUpgradeActionProvider.class);

  /**
   * Package scanned for {@link UpgradeActionOm}-annotated classes (production OM upgrade actions).
   */
  public static final String OM_UPGRADE_CLASS_PACKAGE = "org.apache.hadoop.ozone.om.upgrade";

  public OMUpgradeActionProvider() {
  }

  @Override
  public Map<ComponentVersion, OmUpgradeAction> load() {
    Map<ComponentVersion, OmUpgradeAction> upgradeActions = new HashMap<>();

    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .forPackages(OM_UPGRADE_CLASS_PACKAGE)
        .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner())
        .setExpandSuperTypes(false)
        .setParallel(true));
    Set<Class<?>> typesAnnotatedWith = reflections.getTypesAnnotatedWith(UpgradeActionOm.class);

    typesAnnotatedWith.forEach(actionClass -> {
      if (OmUpgradeAction.class.isAssignableFrom(actionClass)) {
        try {
          OmUpgradeAction action = (OmUpgradeAction) actionClass.getDeclaredConstructor().newInstance();
          UpgradeActionOm annotation = actionClass.getAnnotation(UpgradeActionOm.class);
          OMLayoutFeature feature = annotation.feature();
          LOG.info("Registering Upgrade Action : {}", action.name());
          upgradeActions.put(feature, action);
        } catch (Exception e) {
          LOG.error("Cannot instantiate Upgrade Action class {}",
              actionClass.getSimpleName(), e);
        }
      } else {
        LOG.warn("Found upgrade action class not of type " +
                "org.apache.hadoop.ozone.om.upgrade.OmUpgradeAction : {}",
            actionClass.getName());
      }
    });

    return upgradeActions;
  }
}

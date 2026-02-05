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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.upgrade.AbstractLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutVersionInstanceFactory;
import org.apache.hadoop.ozone.upgrade.VersionFactoryKey;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to manage layout versions and features for Ozone Manager.
 */
public final class OMLayoutVersionManager
    extends AbstractLayoutVersionManager<OMLayoutFeature> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMLayoutVersionManager.class);

  public static final String OM_CLASS_PACKAGE =
      "org.apache.hadoop.ozone.om";
  public static final String OM_REQUEST_CLASS_PACKAGE =
      OM_CLASS_PACKAGE + ".request";
  public static final String OM_UPGRADE_CLASS_PACKAGE =
      OM_CLASS_PACKAGE + ".upgrade";
  private LayoutVersionInstanceFactory<Class<? extends OMClientRequest>>
      requestFactory;

  public OMLayoutVersionManager(int layoutVersion) throws OMException {
    requestFactory = new LayoutVersionInstanceFactory<>();
    init(layoutVersion);
  }

  public OMLayoutVersionManager() throws IOException {
    requestFactory = new LayoutVersionInstanceFactory<>();
    OMLayoutFeature[] features = OMLayoutFeature.values();
    init(features[features.length - 1].layoutVersion(), features);
  }

  /**
   * Initialize the OM Layout Features and current Layout Version.
   * @throws OMException on error.
   */
  private void init(int layoutVersion) throws OMException {
    try {
      init(layoutVersion, OMLayoutFeature.values());
    } catch (IOException e) {
      throw new OMException(
          String.format("Cannot initialize VersionManager. Metadata " +
                  "layout version (%d) > software layout version (%d)",
              getMetadataLayoutVersion(), getSoftwareLayoutVersion()),
          e,
          NOT_SUPPORTED_OPERATION);
    }
    registerUpgradeActions(OM_UPGRADE_CLASS_PACKAGE);
  }

  /**
   * Scan classpath and register all actions to layout features.
   */
  @VisibleForTesting
  protected void registerUpgradeActions(String packageName) {
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .forPackages(packageName)
        .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner())
        .setExpandSuperTypes(false)
        .setParallel(true));
    Set<Class<?>> typesAnnotatedWith =
        reflections.getTypesAnnotatedWith(UpgradeActionOm.class);
    typesAnnotatedWith.forEach(actionClass -> {
      if (OmUpgradeAction.class.isAssignableFrom(actionClass)) {
        try {
          OmUpgradeAction action = (OmUpgradeAction) actionClass.newInstance();
          UpgradeActionOm annotation =
              actionClass.getAnnotation(UpgradeActionOm.class);
          OMLayoutFeature feature = annotation.feature();
          if (feature.layoutVersion() > getMetadataLayoutVersion()) {
            LOG.info("Registering Upgrade Action : {}", action.name());
            feature.addAction(action);
          }  else {
            LOG.info("Skipping Upgrade Action {} since it has been finalized" +
                ".", action.name());
          }
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
  }

  @VisibleForTesting
  public static Set<Class<? extends OMClientRequest>> getRequestClasses(
      String packageName) {
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .setUrls(ClasspathHelper.forPackage(packageName))
        .setScanners(new SubTypesScanner())
        .setExpandSuperTypes(false)
        .setParallel(true));
    Set<Class<? extends OMClientRequest>> validRequests = new HashSet<>();

    Set<Class<? extends OMClientRequest>> subTypes =
        reflections.getSubTypesOf(OMClientRequest.class);
    for (Class<? extends OMClientRequest> requestClass : subTypes) {
      if (!Modifier.isAbstract(requestClass.getModifiers())) {
        validRequests.add(requestClass);
      }
    }
    return validRequests;
  }

  /**
   * Given a type and version, get the corresponding request class type.
   * @param type type string
   * @return class type.
   */
  @Override
  public Class<? extends OMClientRequest> getHandler(String type) {
    VersionFactoryKey versionFactoryKey = new VersionFactoryKey.Builder()
        .key(type).build();
    return requestFactory.get(this, versionFactoryKey);
  }

  @Override
  public void finalized(OMLayoutFeature layoutFeature) {
    super.finalized(layoutFeature);
    requestFactory.finalizeFeature(layoutFeature);
  }

  public static int maxLayoutVersion() {
    OMLayoutFeature[] features = OMLayoutFeature.values();
    return features[features.length - 1].layoutVersion();
  }

}

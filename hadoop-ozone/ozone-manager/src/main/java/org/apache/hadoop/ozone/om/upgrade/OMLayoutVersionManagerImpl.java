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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.upgrade.AbstractLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutVersionInstanceFactory;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.VersionFactoryKey;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class to manage layout versions and features for Ozone Manager.
 */
public final class OMLayoutVersionManagerImpl
    extends AbstractLayoutVersionManager<OMLayoutFeature>
    implements OmLayoutVersionManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMLayoutVersionManagerImpl.class);

  private static final String OM_REQUEST_CLASS_PACKAGE =
      "org.apache.hadoop.ozone.om.request";
  private static OMLayoutVersionManagerImpl omVersionManager;
  private LayoutVersionInstanceFactory<Class<? extends OMClientRequest>>
      requestFactory;

  private OMLayoutVersionManagerImpl() {
    requestFactory = new LayoutVersionInstanceFactory<>();
  }

  /**
   * Read only instance to OM Version Manager.
   * @return version manager instance.
   */
  public static synchronized LayoutVersionManager getInstance() {
    if (omVersionManager == null) {
      throw new RuntimeException("OM Layout Version Manager not yet " +
          "initialized.");
    }
    return omVersionManager;
  }


  /**
   * Initialize OM version manager from storage.
   * @return version manager instance.
   */
  public static synchronized OMLayoutVersionManagerImpl initialize(
      OMStorage omStorage)
      throws OMException {
    if (omVersionManager == null) {
      omVersionManager = new OMLayoutVersionManagerImpl();
      omVersionManager.init(omStorage);
    }
    return omVersionManager;
  }

  /**
   * Initialize the OM Layout Features and current Layout Version.
   * @param storage to read the current layout version.
   * @throws OMException on error.
   */
  private void init(Storage storage) throws OMException {
    try {
      init(storage.getLayoutVersion(), OMLayoutFeature.values());
    } catch (IOException e) {
      throw new OMException(
          String.format("Cannot initialize VersionManager. Metadata " +
                  "layout version (%d) > software layout version (%d)",
              metadataLayoutVersion, softwareLayoutVersion),
          e,
          NOT_SUPPORTED_OPERATION);
    }
    registerOzoneManagerRequests();
  }

  @VisibleForTesting
  protected synchronized static void resetLayoutVersionManager() {
    if (omVersionManager != null) {
      omVersionManager.reset();
      omVersionManager = null;
    }
  }

  public void reset() {
    requestFactory = null;
    super.reset();
  }

  private void registerOzoneManagerRequests() {
    try {
      for (Class<? extends OMClientRequest> reqClass : getRequestClasses()) {
        try {
          Method getRequestTypeMethod = reqClass.getMethod(
              "getRequestType");
          String type = (String) getRequestTypeMethod.invoke(null);
          LOG.debug("Registering {} with OmVersionFactory.",
              reqClass.getSimpleName());
          BelongsToLayoutVersion annotation =
              reqClass.getAnnotation(BelongsToLayoutVersion.class);
          if (annotation == null) {
            registerRequestType(type, INITIAL_VERSION.layoutVersion(),
                reqClass);
          } else {
            registerRequestType(type, annotation.value().layoutVersion(),
                reqClass);
          }
        } catch (NoSuchMethodException nsmEx) {
          LOG.warn("Found a class {} with request type not defined. ",
              reqClass.getSimpleName());
        }
      }
    } catch (Exception ex) {
      LOG.error("Exception registering OM client request.", ex);
    }
  }

  @VisibleForTesting
  public static Set<Class<? extends OMClientRequest>> getRequestClasses() {
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .setUrls(ClasspathHelper.forPackage(OM_REQUEST_CLASS_PACKAGE))
        .setScanners(new SubTypesScanner())
        .setExpandSuperTypes(false));
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

  private void registerRequestType(String type, int version,
                                   Class<? extends OMClientRequest> reqClass) {
    VersionFactoryKey key = new VersionFactoryKey.Builder()
        .key(type).version(version).build();
    requestFactory.register(this, key, reqClass);
  }

  /**
   * Given a type and version, get the corresponding request class type.
   * @param type type string
   * @return class type.
   */
  @Override
  public Class<? extends OMClientRequest> getRequestHandler(String type) {
    VersionFactoryKey versionFactoryKey = new VersionFactoryKey.Builder()
        .key(type).build();
    return requestFactory.get(this, versionFactoryKey);
  }

  @Override
  public void finalized(OMLayoutFeature layoutFeature) {
    super.finalized(layoutFeature);
    requestFactory.finalizeFeature(layoutFeature);
  }
}

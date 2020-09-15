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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;

import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.upgrade.AbstractLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutVersionInstanceFactory;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.VersionFactoryKey;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class to manage layout versions and features for Ozone Manager.
 */
public final class OMLayoutVersionManagerImpl
    extends AbstractLayoutVersionManager implements OmLayoutVersionManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMLayoutVersionManagerImpl.class);

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
    init(storage.getLayoutVersion(), OMLayoutFeature.values());

    if (metadataLayoutVersion > softwareLayoutVersion) {
      throw new OMException(
          String.format("Cannot initialize VersionManager. Metadata " +
                  "layout version (%d) > software layout version (%d)",
              metadataLayoutVersion, softwareLayoutVersion),
          NOT_SUPPORTED_OPERATION);
    }
    registerOzoneManagerRequests();
  }

  public void doFinalize(OzoneManager om) {
    super.doFinalize(om);
    requestFactory.onFinalize(this);
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
    Reflections reflections = new Reflections(
        "org.apache.hadoop.ozone.om.request");
    Set<Class<? extends OMClientRequest>> subTypes =
        reflections.getSubTypesOf(OMClientRequest.class);
    try {
      for (Class<? extends OMClientRequest> requestClass : subTypes) {
        if (Modifier.isAbstract(requestClass.getModifiers())) {
          continue;
        }
        try {
          Method getRequestTypeMethod = requestClass.getMethod(
              "getRequestType");
          String type = (String) getRequestTypeMethod.invoke(null);
          LOG.debug("Registering {} with OmVersionFactory.",
              requestClass.getSimpleName());
          BelongsToLayoutVersion annotation =
              requestClass.getAnnotation(BelongsToLayoutVersion.class);
          if (annotation == null) {
            registerRequestType(type, INITIAL_VERSION.layoutVersion(),
                requestClass);
          } else {
            registerRequestType(type, annotation.value().layoutVersion(),
                requestClass);
          }
        } catch (NoSuchMethodException nsmEx) {
          LOG.warn("Found a class {} with request type not defined. ",
              requestClass.getSimpleName());
        }
      }
    } catch (Exception ex) {
      LOG.error("Exception registering OM client request.", ex);
    }
  }

  private void registerRequestType(String type, int version,
                                   Class<? extends OMClientRequest> reqClass) {
    VersionFactoryKey key = new VersionFactoryKey.Builder()
        .key(type).version(version).build();
    requestFactory.register(this, key, reqClass);
  }

  /**
   * Given a type and version, get the corresponding request class type.
   * @param requestType type string
   * @param version version
   * @return class type.
   */
  @Override
  public Class< ? extends OMClientRequest> getRequestHandler(String type) {
    VersionFactoryKey versionFactoryKey = new VersionFactoryKey.Builder()
        .key(type).build();
    return requestFactory.get(this, versionFactoryKey);
  }
}

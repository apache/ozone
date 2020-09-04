/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeatureCatalog.OMLayoutFeature.INITIAL_VERSION;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;

import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.VersionFactoryKey;
import org.apache.hadoop.ozone.upgrade.LayoutVersionInstanceFactory;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Wrapper class for OM around a generic Versioned Instance factory.
 */
public class OmRequestFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(OmRequestFactory.class);
  private LayoutVersionManager layoutVersionManager;

  private LayoutVersionInstanceFactory<Class<? extends OMClientRequest>>
      requestFactory;

  public OmRequestFactory(LayoutVersionManager lvm) {
    this.layoutVersionManager = lvm;
    this.requestFactory = new LayoutVersionInstanceFactory<>(lvm);
    registerOzoneManagerRequests();
  }

  /**
   * Scan the packages in 'ozone.om.request' and register all classes of type
   * 'OMClientRequest' to the factory.
   */
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
        Method getRequestTypeMethod = requestClass.getMethod("getRequestType");
        if (getRequestTypeMethod != null) {
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
        } else {
          LOG.warn("Found request class {} without a configured requestType.",
              requestClass.getSimpleName());
        }
      }
    } catch (Exception ex) {
      LOG.error("Exception registering OM client request.", ex);
    }
  }

  private void registerRequestType(String type, int version,
      Class<? extends OMClientRequest> requestClass) {
    VersionFactoryKey key = new VersionFactoryKey.Builder()
        .key(type).version(version).build();
    requestFactory.register(key, requestClass);
  }

  /**
   * Given an OM Request, get the corresponding request class type.
   * @param omRequest Om request
   * @return class type.
   */
  public Class< ? extends OMClientRequest> getRequestType(OMRequest omRequest) {
    VersionFactoryKey versionFactoryKey = getVersionFactoryKey(omRequest);
    return requestFactory.get(versionFactoryKey);
  }

  /**
   * Given a type and version, get the corresponding request class type.
   * @param requestType type string
   * @param version version
   * @return class type.
   */
  public Class< ? extends OMClientRequest> getRequestType(
      String requestType, int version) {
    VersionFactoryKey versionFactoryKey = new VersionFactoryKey.Builder()
        .key(requestType).version(version).build();
    return requestFactory.get(versionFactoryKey);
  }

  private VersionFactoryKey getVersionFactoryKey(OMRequest omRequest) {
    int version = omRequest.hasLayoutVersion() ?
        Math.toIntExact(omRequest.getLayoutVersion().getVersion()) :
        layoutVersionManager.getMetadataLayoutVersion();
    return new VersionFactoryKey.Builder()
        .version(version)
        .key(omRequest.getCmdType().name())
        .build();
  }

  @VisibleForTesting
  protected LayoutVersionInstanceFactory<Class<? extends OMClientRequest>>
      getRequestFactory() {
    return requestFactory;
  }
}

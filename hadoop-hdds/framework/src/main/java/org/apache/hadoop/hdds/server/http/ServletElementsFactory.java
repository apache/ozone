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

package org.apache.hadoop.hdds.server.http;

import java.util.Map;
import org.apache.hadoop.hdds.server.http.servletbridge.JavaxFilterBridge;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.http.CrossOriginFilter;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.FilterMapping;

/**
 * Factory class which helps to create different types of servlet elements.
 */
public final class ServletElementsFactory {
  private ServletElementsFactory() {
    throw new UnsupportedOperationException(
        "This is utility class and cannot be instantiated");
  }

  public static FilterMapping createFilterMapping(String mappingName,
      String[] urls) {
    FilterMapping filterMapping = new FilterMapping();
    filterMapping.setPathSpecs(urls);
    filterMapping.setDispatches(FilterMapping.ALL);
    filterMapping.setFilterName(mappingName);
    return filterMapping;
  }

  public static FilterHolder createFilterHolder(String filterName,
      String classname, Map<String, String> parameters) {
    FilterHolder holder = new FilterHolder();
    holder.setName(filterName);
    Class<?> filterClass = loadFilterClass(classname);
    if (javax.servlet.Filter.class.isAssignableFrom(filterClass)) {
      // Some hadoop filters still implement javax.servlet.Filter; run them
      // through the bridge so they work inside Jetty EE10 (jakarta). The bridge
      // faithfully forwards everything a filter does to the same request and
      // response objects (setting/adding headers, status, sendError) and lets a
      // filter short-circuit the chain, but it does not carry a request or
      // response the filter wraps and passes downstream (see JavaxFilterBridge).
      // Rather than run an arbitrary (e.g. operator-configured via
      // ozone.http.filter.initializers) javax filter and silently drop its
      // wrapping, reject anything outside the known bridgeable set with an
      // actionable message.
      if (!isBridgeableJavaxFilter(filterClass)) {
        throw new HttpServerConfigurationException("Filter " + classname
            + " implements javax.servlet.Filter, which Ozone only bridges into "
            + "Jetty EE10 (jakarta) for the hadoop-auth AuthenticationFilter "
            + "family, StaticUserWebFilter, CrossOriginFilter and "
            + "RestCsrfPreventionFilter. The bridge does not propagate a request "
            + "or response that a filter wraps and forwards downstream (for "
            + "example XFrameOptionsFilter), so such a wrapper would be silently "
            + "dropped. Provide a jakarta.servlet.Filter instead.");
      }
      holder.setFilter(new JavaxFilterBridge(newJavaxFilter(filterClass)));
    } else {
      holder.setClassName(classname);
    }
    if (parameters != null) {
      holder.setInitParameters(parameters);
    }
    return holder;
  }

  /**
   * A javax filter is bridged into the jakarta chain only when it acts on the
   * same request and response objects it is given - establishing an
   * authenticated principal, setting response headers, or short-circuiting the
   * chain - which is what the hadoop-auth {@link AuthenticationFilter} family
   * (SPNEGO, Kerberos, delegation token), {@link StaticUserWebFilter},
   * {@link CrossOriginFilter} (CORS response headers) and
   * {@link RestCsrfPreventionFilter} (rejects or forwards unchanged) all do.
   * Filters that wrap the request or response and forward the wrapper downstream
   * (for example {@code XFrameOptionsFilter}) are not bridgeable, because the
   * bridge does not carry that wrapper into the jakarta chain.
   */
  private static boolean isBridgeableJavaxFilter(Class<?> filterClass) {
    return AuthenticationFilter.class.isAssignableFrom(filterClass)
        || StaticUserWebFilter.StaticUserFilter.class
            .isAssignableFrom(filterClass)
        || CrossOriginFilter.class.isAssignableFrom(filterClass)
        || RestCsrfPreventionFilter.class.isAssignableFrom(filterClass);
  }

  private static Class<?> loadFilterClass(String classname) {
    try {
      return Class.forName(classname, false,
          Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException e) {
      throw new HttpServerConfigurationException("Filter class not found: " + classname, e);
    }
  }

  private static javax.servlet.Filter newJavaxFilter(Class<?> filterClass) {
    try {
      return (javax.servlet.Filter) filterClass.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new HttpServerConfigurationException(
          "Unable to instantiate filter: " + filterClass.getName(), e);
    }
  }
}

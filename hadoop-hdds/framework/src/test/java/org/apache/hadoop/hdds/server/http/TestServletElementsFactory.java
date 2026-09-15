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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hdds.server.http.servletbridge.JavaxFilterBridge;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.http.CrossOriginFilter;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ServletElementsFactory#createFilterHolder}, focused on
 * which filters are bridged from javax into the Jetty EE10 (jakarta) chain. The
 * bridge carries whatever a filter does to the request and response it is given,
 * so the hadoop-auth {@link AuthenticationFilter} family, {@link StaticUserWebFilter},
 * {@link CrossOriginFilter} and {@link RestCsrfPreventionFilter} may be bridged;
 * a javax filter that wraps the request or response and forwards the wrapper
 * downstream is rejected rather than run with that wrapper silently dropped.
 */
class TestServletElementsFactory {

  @Test
  void bridgesHadoopAuthFilter() {
    FilterHolder holder = ServletElementsFactory.createFilterHolder(
        "auth", AuthenticationFilter.class.getName(), null);
    // setFilter stores the instance; getHeldClass reflects its type before the
    // holder is started (getFilter is only populated at doStart).
    assertEquals(JavaxFilterBridge.class, holder.getHeldClass(),
        "hadoop-auth AuthenticationFilter must be run through the bridge");
  }

  @Test
  void bridgesStaticUserFilter() {
    FilterHolder holder = ServletElementsFactory.createFilterHolder(
        "static", StaticUserWebFilter.StaticUserFilter.class.getName(), null);
    assertEquals(JavaxFilterBridge.class, holder.getHeldClass(),
        "StaticUserFilter must be run through the bridge");
  }

  @Test
  void bridgesCrossOriginFilter() {
    FilterHolder holder = ServletElementsFactory.createFilterHolder(
        "cors", CrossOriginFilter.class.getName(), null);
    assertEquals(JavaxFilterBridge.class, holder.getHeldClass(),
        "CrossOriginFilter sets CORS response headers on the given response "
            + "and must be run through the bridge");
  }

  @Test
  void bridgesRestCsrfPreventionFilter() {
    FilterHolder holder = ServletElementsFactory.createFilterHolder(
        "csrf", RestCsrfPreventionFilter.class.getName(), null);
    assertEquals(JavaxFilterBridge.class, holder.getHeldClass(),
        "RestCsrfPreventionFilter rejects or forwards the request unchanged "
            + "and must be run through the bridge");
  }

  @Test
  void registersJakartaFilterByClassName() {
    FilterHolder holder = ServletElementsFactory.createFilterHolder(
        "jakarta", PassthroughJakartaFilter.class.getName(), null);
    assertNull(holder.getHeldClass(), "a jakarta filter must not be bridged");
    assertEquals(PassthroughJakartaFilter.class.getName(),
        holder.getClassName());
  }

  @Test
  void rejectsUnknownJavaxFilter() {
    // HttpServerConfigurationException makes the misconfiguration fatal so the
    // service does not silently come up without an HTTP server.
    HttpServerConfigurationException e = assertThrows(
        HttpServerConfigurationException.class,
        () -> ServletElementsFactory.createFilterHolder(
            "custom", CustomJavaxFilter.class.getName(), null));
    // The message must point the operator at the jakarta migration.
    assertTrue(e.getMessage().contains("javax.servlet.Filter"), e.getMessage());
    assertTrue(e.getMessage().contains("jakarta.servlet.Filter"), e.getMessage());
  }

  @Test
  void rejectsUnknownFilterClass() {
    // The class name is resolved eagerly (Jetty 12 used to resolve it at start),
    // so an unknown filter class must fail here with an actionable message.
    HttpServerConfigurationException e = assertThrows(
        HttpServerConfigurationException.class,
        () -> ServletElementsFactory.createFilterHolder(
            "missing", "org.apache.hadoop.hdds.server.http.NoSuchFilterClass", null));
    assertTrue(e.getMessage().contains("Filter class not found"), e.getMessage());
  }

  @Test
  void rejectsNonInstantiableBridgeableFilter() {
    // A bridgeable javax filter that cannot be reflectively instantiated must
    // fail with an actionable message rather than a raw reflection error.
    HttpServerConfigurationException e = assertThrows(
        HttpServerConfigurationException.class,
        () -> ServletElementsFactory.createFilterHolder(
            "broken", NonInstantiableJavaxFilter.class.getName(), null));
    assertTrue(e.getMessage().contains("Unable to instantiate filter"), e.getMessage());
  }

  /** A javax filter outside the bridged hadoop-auth family. */
  public static class CustomJavaxFilter implements javax.servlet.Filter {
    @Override
    public void init(javax.servlet.FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(javax.servlet.ServletRequest request,
        javax.servlet.ServletResponse response,
        javax.servlet.FilterChain chain) {
    }

    @Override
    public void destroy() {
    }
  }

  /**
   * A bridgeable javax filter (it extends {@link CrossOriginFilter}) whose only
   * constructor is private, so the reflective no-arg instantiation in
   * createFilterHolder fails and must be reported with an actionable message.
   */
  public static final class NonInstantiableJavaxFilter extends CrossOriginFilter {
    private NonInstantiableJavaxFilter() {
    }
  }

  /** A jakarta filter, which is registered by class name (no bridge). */
  public static class PassthroughJakartaFilter implements jakarta.servlet.Filter {
    @Override
    public void doFilter(jakarta.servlet.ServletRequest request,
        jakarta.servlet.ServletResponse response,
        jakarta.servlet.FilterChain chain)
        throws java.io.IOException, jakarta.servlet.ServletException {
      chain.doFilter(request, response);
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.recon.api.filters;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.eclipse.jetty.servlet.FilterHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HEATMAP_PROVIDER_KEY;

/**
 * Filter class acts as framework to apply filters to endpoint paths which are
 * dependent on internal components and not readily available.
 */
@Singleton
public class ReconInternalComponentFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconInternalComponentFilter.class);

  private final OzoneConfiguration conf;
  private HeatMapFilter heatMapFilter;

  @Inject
  ReconInternalComponentFilter(OzoneConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // This is the beginning of usage of @InternalOnly annotation, if in future
    // any other endpoint uses this annotation which means that respective
    // feature is dependent on internal component which is not readily available
    // so accordingly its own filter to be added and configured. So this class
    // acts as framework for all endpoints tagged as internal features.
    heatMapFilter = new HeatMapFilter();
    Map<String, String> parameters = getHeatMapFilterConfigMap();
    FilterHolder filterHolder = getFilterHolder("heatmap",
        HeatMapFilter.class.getName(),
        parameters);
    heatMapFilter.init(new FilterConfig() {
      @Override
      public String getFilterName() {
        return filterHolder.getName();
      }

      @Override
      public ServletContext getServletContext() {
        return filterConfig.getServletContext();
      }

      @Override
      public String getInitParameter(String s) {
        return filterHolder.getInitParameter(s);
      }

      @Override
      public Enumeration<String> getInitParameterNames() {
        return filterHolder.getInitParameterNames();
      }
    });
  }

  private Map<String, String> getHeatMapFilterConfigMap() {
    // This can be map of other parameters (can be added in future) if a
    // heatmap feature have dependency on other parameters as well.
    Map<String, String> parameters = new HashMap<>();
    parameters.put(OZONE_RECON_HEATMAP_PROVIDER_KEY,
        conf.get(OZONE_RECON_HEATMAP_PROVIDER_KEY));
    return parameters;
  }

  @Override
  public void doFilter(ServletRequest servletRequest,
      ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Filtering request to {} through internal feature filter.",
          ((HttpServletRequest) servletRequest).getRequestURL());
    }
    // There can be multiple internal features who can use {@link InternalOnly}
    // so individual internal features should apply here their filter.
    heatMapFilter.doFilter(servletRequest, servletResponse, filterChain);
  }

  private static FilterHolder getFilterHolder(String name, String classname,
                                              Map<String, String> parameters) {
    FilterHolder holder = new FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    if (parameters != null) {
      holder.setInitParameters(parameters);
    }
    return holder;
  }

  @Override
  public void destroy() { }
}

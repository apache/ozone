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

import org.apache.hadoop.ozone.recon.heatmap.IHeatMapProvider;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HEATMAP_PROVIDER_KEY;
import static org.apache.hadoop.ozone.recon.ReconUtils.loadHeatMapProvider;

/**
 * This is a filter for internal heatmap feature.
 */
public class HeatMapFilter implements Filter {
  private static Logger log = LoggerFactory.getLogger(HeatMapFilter.class);

  private String heatMapProviderImplCls;

  public HeatMapFilter() {
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    heatMapProviderImplCls = filterConfig.
        getInitParameter(OZONE_RECON_HEATMAP_PROVIDER_KEY);
  }

  @Override
  public void doFilter(ServletRequest servletRequest,
                       ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {
    String reason;
    int errCode = HttpStatus.SC_FAILED_DEPENDENCY;
    HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;
    try {
      IHeatMapProvider iHeatMapProvider =
          loadHeatMapProvider(heatMapProviderImplCls);
      String startDate = String.valueOf(Instant.now().toEpochMilli() - 100000);
      if (null == iHeatMapProvider.retrieveData("/", "key", startDate)) {
        reason = "HeatMapProvider implementation not available.";
        httpResponse.setStatus(errCode, reason);
        httpResponse.sendError(errCode, reason);
      } else {
        filterChain.doFilter(servletRequest, servletResponse);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void destroy() {

  }
}

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

import org.apache.commons.lang3.StringUtils;
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

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_SOLR_ADDRESS_KEY;

/**
 * This is a filter for internal heatmap feature.
 */
public class HeatMapFilter implements Filter {
  private static Logger log = LoggerFactory.getLogger(HeatMapFilter.class);

  private String solrAddressForHeatMap;

  public HeatMapFilter() {
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    solrAddressForHeatMap = filterConfig.
        getInitParameter(OZONE_SOLR_ADDRESS_KEY);
  }

  @Override
  public void doFilter(ServletRequest servletRequest,
                       ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {
    String reason;
    int errCode = HttpStatus.SC_FAILED_DEPENDENCY;
    HttpServletResponse httpResponse = (HttpServletResponse)servletResponse;
    if (StringUtils.isEmpty(solrAddressForHeatMap)) {
      reason = "Solr Address Not Configured";
      httpResponse.setStatus(errCode, reason);
      httpResponse.sendError(errCode, reason);
    } else {
      filterChain.doFilter(servletRequest, servletResponse);
    }
  }

  @Override
  public void destroy() {

  }
}

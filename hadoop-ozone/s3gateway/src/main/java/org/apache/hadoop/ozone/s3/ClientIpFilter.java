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

package org.apache.hadoop.ozone.s3;

import jakarta.annotation.Priority;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter used to get ClientIP from HttpServletRequest.
 */

@Provider
@PreMatching
@Priority(ClientIpFilter.PRIORITY)
public class ClientIpFilter implements ContainerRequestFilter {

  public static final int PRIORITY = HeaderPreprocessor.PRIORITY +
      S3GatewayHttpServer.FILTER_PRIORITY_DO_AFTER;

  public static final String CLIENT_IP_HEADER = "client_ip";

  private static final Logger LOG = LoggerFactory.getLogger(
      ClientIpFilter.class);

  @Context
  private HttpServletRequest httpServletRequest;

  @Override
  public void filter(ContainerRequestContext request) throws IOException {
    String clientIp = httpServletRequest.getHeader("x-real-ip");

    if (clientIp == null || clientIp.isEmpty()) {
      // extract from forward ips
      String ipForwarded = httpServletRequest.getHeader("x-forwarded-for");
      String[] ips = ipForwarded == null ? null : ipForwarded.split(",");
      clientIp = (ips == null || ips.length == 0) ? null : ips[0];

      // extract from remote addr
      clientIp = (clientIp == null || clientIp.isEmpty()) ?
          httpServletRequest.getRemoteAddr() : clientIp;
    }
    LOG.trace("Real Ip[{}]", clientIp);
    request.getHeaders().putSingle(CLIENT_IP_HEADER, clientIp);
  }

}

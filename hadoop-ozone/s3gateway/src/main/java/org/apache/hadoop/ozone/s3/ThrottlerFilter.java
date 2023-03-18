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
package org.apache.hadoop.ozone.s3;


import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.throttler.Request;
import org.apache.hadoop.ozone.s3.throttler.RequestScheduler;
import org.apache.hadoop.ozone.s3.throttler.ThrottlerUtils;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * Filter to reject incoming requests from specific users based on scheduler
 * criteria.
 */
@Provider
public class ThrottlerFilter implements
    ContainerRequestFilter, ContainerResponseFilter {
  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Inject
  private RequestScheduler scheduler;

  @Override
  public void filter(ContainerRequestContext ctx) throws IOException {
    Request request = getRequests(ctx);

    if (scheduler.shouldReject(request)) {
      ctx.abortWith(Response
          .status(Response.Status.SERVICE_UNAVAILABLE)
          .entity("Too many requests")
          .build());
    } else {
      scheduler.addRequest(request);
    }
  }

  @Override
  public void filter(ContainerRequestContext requestContext,
                     ContainerResponseContext responseContext)
      throws IOException {
    scheduler.removeRequest(getRequests(requestContext));
  }

  private Request getRequests(ContainerRequestContext ctx) {
    return ThrottlerUtils.toRequest(ctx, ozoneConfiguration);
  }
}

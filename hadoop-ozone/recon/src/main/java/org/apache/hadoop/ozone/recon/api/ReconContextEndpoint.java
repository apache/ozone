/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.ozone.recon.ReconContext;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * REST API to expose ReconContext information for admin users.
 */
@Path("/reconcontext")
@Produces(MediaType.APPLICATION_JSON)
public class ReconContextEndpoint {

  private final ReconContext reconContext;

  @Inject
  public ReconContextEndpoint(ReconContext reconContext) {
    this.reconContext = reconContext;
  }

  /**
   * API to get the overall health status of Recon.
   *
   * @return HTTP Response containing the health status and other info.
   */
  @GET
  @Path("/status")
  public Response getReconContextStatus() {
    return Response.ok(reconContext.toMap()).build();
  }

  /**
   * API to get detailed error information from ReconContext.
   *
   * @return HTTP Response containing the list of errors recorded during startup.
   */
  @GET
  @Path("/errors")
  public Response getReconErrors() {
    return Response.ok(reconContext.getErrors()).build();
  }

  /**
   * API to get the health status of Recon as a simple boolean.
   *
   * @return HTTP Response containing the health status.
   */
  @GET
  @Path("/health")
  public Response isReconHealthy() {
    return Response.ok(reconContext.isHealthy().get()).build();
  }
}

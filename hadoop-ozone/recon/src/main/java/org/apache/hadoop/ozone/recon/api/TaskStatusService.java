/*
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

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.ozone.recon.metrics.ReconTaskStatusCounter;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import java.util.List;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_ENTITY_TYPE;

/**
 * Endpoint for displaying the last successful run of each Recon Task.
 */
@Path("/task")
@Produces(MediaType.APPLICATION_JSON)
public class TaskStatusService {

  @Inject
  private ReconTaskStatusDao reconTaskStatusDao;
  @Inject
  private ReconTaskStatusCounter taskStatusCounter;

  /**
   * Return the list of Recon Tasks and the last successful timestamp and
   * sequence number.
   * @return {@link Response}
   */
  @GET
  @Path("status")
  public Response getTaskTimes() {
    List<ReconTaskStatus> resultSet = reconTaskStatusDao.findAll();
    return Response.ok(resultSet).build();
  }

  @GET
  @Path("metrics")
  public Response getTaskMetrics() {
    return Response.ok(taskStatusCounter.getTaskCounts()).build();
  }

  @GET
  @Path("metrics")
  public Response getTaskMetrics(
    @QueryParam("taskName") String taskName
  ) {
    return Response.ok(taskStatusCounter.getTaskStatusCounts(taskName)).build();
  }
}

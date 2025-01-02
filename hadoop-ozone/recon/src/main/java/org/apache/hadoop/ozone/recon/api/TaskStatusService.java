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

import org.apache.hadoop.ozone.recon.api.types.ReconTaskStatusResponse;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Endpoint for displaying the last successful run of each Recon Task.
 */
@Path("/task")
@Produces(MediaType.APPLICATION_JSON)
public class TaskStatusService {

  private ReconTaskStatusDao reconTaskStatusDao;

  @Inject
  TaskStatusService(ReconTaskStatusDao reconTaskStatusDao) {
    this.reconTaskStatusDao = reconTaskStatusDao;
  }

  // Internal function to combine counter value with DerbyDB values
  private ReconTaskStatusResponse convertToTaskStatusResponse(ReconTaskStatus task) {
    return new ReconTaskStatusResponse(
        task.getTaskName(), task.getLastUpdatedSeqNumber(), task.getLastUpdatedTimestamp(),
        task.getIsCurrentTaskRunning(), task.getLastTaskRunStatus());
  }

  /**
   * Return the list of Recon Tasks and associated stats.
   * @return {@link Response}
   */
  @GET
  @Path("status")
  public Response getTaskStats() {
    List<ReconTaskStatus> resultSet = reconTaskStatusDao.findAll();
    List<ReconTaskStatusResponse> taskStatsList = resultSet.stream().map(
        this::convertToTaskStatusResponse).collect(Collectors.toList());
    return Response.ok(taskStatsList).build();
  }
}

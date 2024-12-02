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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.recon.api.types.ReconTaskStatusCountResponse;
import org.apache.hadoop.ozone.recon.metrics.ReconTaskStatusCounter;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_ENTITY_TYPE;

/**
 * Endpoint for displaying the last successful run of each Recon Task.
 */
@Path("/task")
@Produces(MediaType.APPLICATION_JSON)
public class TaskStatusService {

  @Inject
  private ReconTaskStatusDao reconTaskStatusDao;
  private final ReconTaskStatusCounter taskStatusCounter = ReconTaskStatusCounter.getCurrentInstance();

  /**
   * Internal class to represent a list of all the tasks and their counts in Recon
   */
  static class ReconAllTasksCountResponse {
    // An array of all the tasks and their respective successes/failures
    @JsonProperty("tasks")
    private List<ReconTaskStatusCountResponse> tasks;
    ReconAllTasksCountResponse(List<ReconTaskStatusCountResponse> tasks) {
      this.tasks = tasks;
    }
  }

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
    Map<ReconTaskStatusCounter.ReconTasks, Pair<Integer, Integer>> tasksPairMap = taskStatusCounter.getTaskCounts();
    List<ReconTaskStatusCountResponse> tasks = new ArrayList<>();
    for (Map.Entry<ReconTaskStatusCounter.ReconTasks, Pair<Integer, Integer>> entry: tasksPairMap.entrySet()) {
      tasks.add(new ReconTaskStatusCountResponse(
        entry.getKey().name(),
        entry.getValue().getLeft(),
        entry.getValue().getRight()
      ));
    }
    return Response.ok(new ReconAllTasksCountResponse(tasks)).build();
  }

  @GET
  @Path("metrics/{taskName}")
  public Response getTaskMetrics(
    @PathParam("taskName") String taskName
  ) {
    try {
      Pair<Integer, Integer> taskCounts = taskStatusCounter.getTaskStatusCounts(taskName);
      ReconTaskStatusCountResponse taskStatusResponse = new ReconTaskStatusCountResponse(
        taskName,
        taskCounts.getLeft(),
        taskCounts.getRight()
      );
      return Response.ok(taskStatusResponse).build();
    } catch (NullPointerException npe) {
      return Response.status(500, npe.getMessage()).build();
    }
  }
}

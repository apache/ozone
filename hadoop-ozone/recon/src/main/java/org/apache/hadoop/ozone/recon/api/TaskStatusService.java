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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
   * Internal class to represent a list of all the tasks and their counts in Recon.
   */
  static class ReconAllTasksCountResponse {
    // An array of all the tasks and their respective successes/failures
    @JsonProperty("tasks")
    private final List<ReconTaskStatusCountResponse> tasks;

    ReconAllTasksCountResponse(List<ReconTaskStatusCountResponse> tasks) {
      this.tasks = tasks;
    }

    public List<ReconTaskStatusCountResponse> getTasks() {
      return tasks;
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

  /**
   * Returns a list of tasks mapped using their task names to the number of
   * successful runs and number of failed runs for the respective task.
   * @return {@link Response} in the format of:
   * <br>
   * <code>
   *   {
   *    "tasks": [{
   *      "taskName": task name,
   *      "successes": count of successful runs,
   *      "failures": count of failed runs
   *    }, {....}]
   *   }
   * </code>
   */
  @GET
  @Path("stats")
  public Response getTaskStatusStats() {
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
}

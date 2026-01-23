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

package org.apache.hadoop.ozone.recon.api;

import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ReconTaskStatus;

/**
 * Endpoint for displaying the last successful run of each Recon Task.
 */
@Path("/task")
@Produces(MediaType.APPLICATION_JSON)
public class TaskStatusService {

  @Inject
  private ReconTaskStatusDao reconTaskStatusDao;

  /**
   * Return the list of Recon Tasks and their related stats from RECON_TASK_STATUS table.
   * @return {@link Response}
   */
  @GET
  @Path("status")
  public Response getTaskStats() {
    List<ReconTaskStatus> resultSet = reconTaskStatusDao.findAll();
    return Response.ok(resultSet).build();
  }
}

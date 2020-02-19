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

package org.apache.hadoop.ozone.recon.api;

import javax.inject.Inject;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Endpoint for querying the counts of a certain file Size.
 */
@Path("/utilization")
@Produces(MediaType.APPLICATION_JSON)
public class UtilizationService {

  @Inject
  private FileCountBySizeDao fileCountBySizeDao;

  /**
   * Return the file counts from Recon DB.
   * @return {@link Response}
   */
  @GET
  @Path("/fileCount")
  public Response getFileCounts() {
    List<FileCountBySize> resultSet = fileCountBySizeDao.findAll();
    return Response.ok(resultSet).build();
  }
}

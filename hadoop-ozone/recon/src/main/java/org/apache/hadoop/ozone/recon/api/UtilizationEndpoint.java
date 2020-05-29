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

import javax.inject.Inject;

import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.jooq.DSLContext;
import org.jooq.Record3;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_BUCKET;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_FILE_SIZE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_VOLUME;
import static org.hadoop.ozone.recon.schema.tables.FileCountBySizeTable.FILE_COUNT_BY_SIZE;

/**
 * Endpoint for querying the counts of a certain file Size.
 */
@Path("/utilization")
@Produces(MediaType.APPLICATION_JSON)
public class UtilizationEndpoint {

  private FileCountBySizeDao fileCountBySizeDao;
  private UtilizationSchemaDefinition utilizationSchemaDefinition;

  @Inject
  public UtilizationEndpoint(FileCountBySizeDao fileCountBySizeDao,
                             UtilizationSchemaDefinition
                                 utilizationSchemaDefinition) {
    this.utilizationSchemaDefinition = utilizationSchemaDefinition;
    this.fileCountBySizeDao = fileCountBySizeDao;
  }

  /**
   * Return the file counts from Recon DB.
   * @return {@link Response}
   */
  @GET
  @Path("/fileCount")
  public Response getFileCounts(
      @QueryParam(RECON_QUERY_VOLUME)
          String volume,
      @QueryParam(RECON_QUERY_BUCKET)
          String bucket,
      @QueryParam(RECON_QUERY_FILE_SIZE)
          long fileSize
  ) {
    DSLContext dslContext = utilizationSchemaDefinition.getDSLContext();
    List<FileCountBySize> resultSet;
    if (volume != null && bucket != null && fileSize > 0) {
      Record3<String, String, Long> recordToFind = dslContext
          .newRecord(FILE_COUNT_BY_SIZE.VOLUME,
              FILE_COUNT_BY_SIZE.BUCKET,
              FILE_COUNT_BY_SIZE.FILE_SIZE)
          .value1(volume)
          .value2(bucket)
          .value3(fileSize);
      FileCountBySize record = fileCountBySizeDao.findById(recordToFind);
      resultSet = record != null ?
          Collections.singletonList(record) : Collections.emptyList();
    } else if (volume != null && bucket != null) {
      resultSet = dslContext.select().from(FILE_COUNT_BY_SIZE)
          .where(FILE_COUNT_BY_SIZE.VOLUME.eq(volume))
          .and(FILE_COUNT_BY_SIZE.BUCKET.eq(bucket))
          .fetchInto(FileCountBySize.class);
    } else if (volume != null) {
      resultSet = fileCountBySizeDao.fetchByVolume(volume);
    } else {
      // fetch all records
      resultSet = fileCountBySizeDao.findAll();
    }
    return Response.ok(resultSet).build();
  }
}

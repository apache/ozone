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

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_BUCKET;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_CONTAINER_SIZE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_FILE_SIZE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_VOLUME;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.spi.ReconContainerSizeMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.ContainerSizeCountKey;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountKey;
import org.apache.ozone.recon.schema.generated.tables.pojos.ContainerCountBySize;
import org.apache.ozone.recon.schema.generated.tables.pojos.FileCountBySize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Endpoint for querying the counts of a certain file Size.
 */
@Path("/utilization")
@Produces(MediaType.APPLICATION_JSON)
public class UtilizationEndpoint {

  private ReconFileMetadataManager reconFileMetadataManager;
  private ReconContainerSizeMetadataManager reconContainerSizeMetadataManager;
  private static final Logger LOG = LoggerFactory
      .getLogger(UtilizationEndpoint.class);

  @Inject
  public UtilizationEndpoint(
      ReconFileMetadataManager reconFileMetadataManager,
      ReconContainerSizeMetadataManager reconContainerSizeMetadataManager) {
    this.reconFileMetadataManager = reconFileMetadataManager;
    this.reconContainerSizeMetadataManager = reconContainerSizeMetadataManager;
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
    List<FileCountBySize> resultSet = new ArrayList<>();
    try {
      Table<FileSizeCountKey, Long> fileCountTable = reconFileMetadataManager.getFileCountTable();
      
      if (volume != null && bucket != null && fileSize > 0) {
        // Query for specific volume, bucket, and file size
        FileSizeCountKey key = new FileSizeCountKey(volume, bucket, fileSize);
        Long count = fileCountTable.get(key);
        if (count != null && count > 0) {
          FileCountBySize record = new FileCountBySize();
          record.setVolume(volume);
          record.setBucket(bucket);
          record.setFileSize(fileSize);
          record.setCount(count);
          resultSet.add(record);
        }
      } else {
        // Use iterator to scan through all records and filter
        try (KeyValueIterator<FileSizeCountKey, Long> iterator = fileCountTable.iterator()) {
          while (iterator.hasNext()) {
            Table.KeyValue<FileSizeCountKey, Long> entry = iterator.next();
            FileSizeCountKey key = entry.getKey();
            Long count = entry.getValue();
            
            // Apply filters
            boolean matches = true;
            if (volume != null && !volume.equals(key.getVolume())) {
              matches = false;
            }
            if (bucket != null && !bucket.equals(key.getBucket())) {
              matches = false;
            }
            
            if (matches && count != null && count > 0) {
              FileCountBySize record = new FileCountBySize();
              record.setVolume(key.getVolume());
              record.setBucket(key.getBucket());
              record.setFileSize(key.getFileSizeUpperBound());
              record.setCount(count);
              resultSet.add(record);
            }
          }
        }
      }
      return Response.ok(resultSet).build();
    } catch (Exception e) {
      LOG.error("Error retrieving file counts from RocksDB", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * Return the container size counts from RocksDB.
   *
   * @return {@link Response}
   */
  @GET
  @Path("/containerCount")
  public Response getContainerCounts(
      @QueryParam(RECON_QUERY_CONTAINER_SIZE)
          long upperBound) {
    List<ContainerCountBySize> resultSet = new ArrayList<>();
    try {
      Table<ContainerSizeCountKey, Long> containerCountTable =
          reconContainerSizeMetadataManager.getContainerCountTable();

      Long containerSizeUpperBound =
          ReconUtils.getContainerSizeUpperBound(upperBound);

      if (upperBound > 0) {
        // Query for specific container size
        ContainerSizeCountKey key =
            new ContainerSizeCountKey(containerSizeUpperBound);
        Long count = containerCountTable.get(key);
        if (count != null && count > 0) {
          ContainerCountBySize record = new ContainerCountBySize();
          record.setContainerSize(containerSizeUpperBound);
          record.setCount(count);
          resultSet.add(record);
        }
      } else {
        // Iterate through all records
        try (KeyValueIterator<ContainerSizeCountKey, Long> iterator =
            containerCountTable.iterator()) {
          while (iterator.hasNext()) {
            Table.KeyValue<ContainerSizeCountKey, Long> entry = iterator.next();
            ContainerSizeCountKey key = entry.getKey();
            Long count = entry.getValue();

            if (count != null && count > 0) {
              ContainerCountBySize record = new ContainerCountBySize();
              record.setContainerSize(key.getContainerSizeUpperBound());
              record.setCount(count);
              resultSet.add(record);
            }
          }
        }
      }
      return Response.ok(resultSet).build();
    } catch (Exception e) {
      LOG.error("Error retrieving container counts from RocksDB", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

}

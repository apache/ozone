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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.ContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata.ContainerBlockMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeysResponse;
import org.apache.hadoop.ozone.recon.api.types.MissingContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.MissingContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersSummary;
import org.apache.hadoop.ozone.recon.persistence.ContainerHistory;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.hadoop.ozone.recon.schema.tables.pojos.UnhealthyContainers;

import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_BATCH_NUMBER;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.PREV_CONTAINER_ID_DEFAULT_VALUE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_BATCH_PARAM;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;


/**
 * Endpoint for querying keys that belong to a container.
 */
@Path("/containers")
@Produces(MediaType.APPLICATION_JSON)
public class ContainerEndpoint {

  @Inject
  private ContainerDBServiceProvider containerDBServiceProvider;

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  private final ReconContainerManager containerManager;
  private final ContainerHealthSchemaManager containerHealthSchemaManager;

  @Inject
  public ContainerEndpoint(OzoneStorageContainerManager reconSCM,
      ContainerHealthSchemaManager containerHealthSchemaManager) {
    this.containerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.containerHealthSchemaManager = containerHealthSchemaManager;
  }

  /**
   * Return @{@link org.apache.hadoop.ozone.recon.api.types.ContainerMetadata}
   * for the containers starting from the given "prev-key" query param for the
   * given "limit". The given "prev-key" is skipped from the results returned.
   *
   * @param limit max no. of containers to get.
   * @param prevKey the containerID after which results are returned.
   * @return {@link Response}
   */
  @GET
  public Response getContainers(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
          int limit,
      @DefaultValue(PREV_CONTAINER_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_PREVKEY) long prevKey) {
    Map<Long, ContainerMetadata> containersMap;
    long containersCount;
    try {
      containersMap = containerDBServiceProvider.getContainers(limit, prevKey);
      containersCount = containerDBServiceProvider.getCountForContainers();
    } catch (IOException ioEx) {
      throw new WebApplicationException(ioEx,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    ContainersResponse containersResponse =
        new ContainersResponse(containersCount, containersMap.values());
    return Response.ok(containersResponse).build();
  }

  /**
   * Return @{@link org.apache.hadoop.ozone.recon.api.types.KeyMetadata} for
   * all keys that belong to the container identified by the id param
   * starting from the given "prev-key" query param for the given "limit".
   * The given prevKeyPrefix is skipped from the results returned.
   *
   * @param containerID the given containerID.
   * @param limit max no. of keys to get.
   * @param prevKeyPrefix the key prefix after which results are returned.
   * @return {@link Response}
   */
  @GET
  @Path("/{id}/keys")
  public Response getKeysForContainer(
      @PathParam("id") Long containerID,
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
          int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
          String prevKeyPrefix) {
    Map<String, KeyMetadata> keyMetadataMap = new LinkedHashMap<>();
    long totalCount;
    try {
      Map<ContainerKeyPrefix, Integer> containerKeyPrefixMap =
          containerDBServiceProvider.getKeyPrefixesForContainer(containerID,
              prevKeyPrefix);

      // Get set of Container-Key mappings for given containerId.
      for (ContainerKeyPrefix containerKeyPrefix : containerKeyPrefixMap
          .keySet()) {

        // Directly calling get() on the Key table instead of iterating since
        // only full keys are supported now. When we change to using a prefix
        // of the key, this needs to change to prefix seek.
        OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().getSkipCache(
            containerKeyPrefix.getKeyPrefix());
        if (null != omKeyInfo) {
          // Filter keys by version.
          List<OmKeyLocationInfoGroup> matchedKeys = omKeyInfo
              .getKeyLocationVersions()
              .stream()
              .filter(k -> (k.getVersion() ==
                  containerKeyPrefix.getKeyVersion()))
              .collect(Collectors.toList());

          List<ContainerBlockMetadata> blockIds =
              getBlocks(matchedKeys, containerID);

          String ozoneKey = omMetadataManager.getOzoneKey(
              omKeyInfo.getVolumeName(),
              omKeyInfo.getBucketName(),
              omKeyInfo.getKeyName());
          if (keyMetadataMap.containsKey(ozoneKey)) {
            keyMetadataMap.get(ozoneKey).getVersions()
                .add(containerKeyPrefix.getKeyVersion());

            keyMetadataMap.get(ozoneKey).getBlockIds()
                .put(containerKeyPrefix.getKeyVersion(), blockIds);
          } else {
            // break the for loop if limit has been reached
            if (keyMetadataMap.size() == limit) {
              break;
            }
            KeyMetadata keyMetadata = new KeyMetadata();
            keyMetadata.setBucket(omKeyInfo.getBucketName());
            keyMetadata.setVolume(omKeyInfo.getVolumeName());
            keyMetadata.setKey(omKeyInfo.getKeyName());
            keyMetadata.setCreationTime(
                Instant.ofEpochMilli(omKeyInfo.getCreationTime()));
            keyMetadata.setModificationTime(
                Instant.ofEpochMilli(omKeyInfo.getModificationTime()));
            keyMetadata.setDataSize(omKeyInfo.getDataSize());
            keyMetadata.getVersions().add(containerKeyPrefix.getKeyVersion());
            keyMetadataMap.put(ozoneKey, keyMetadata);
            keyMetadata.getBlockIds().put(containerKeyPrefix.getKeyVersion(),
                blockIds);
          }
        }
      }

      totalCount =
          containerDBServiceProvider.getKeyCountForContainer(containerID);
    } catch (IOException ioEx) {
      throw new WebApplicationException(ioEx,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    KeysResponse keysResponse =
        new KeysResponse(totalCount, keyMetadataMap.values());
    return Response.ok(keysResponse).build();
  }

  /**
   * Return Container replica history for the container identified by the id
   * param.
   *
   * @param containerID the given containerID.
   * @return {@link Response}
   */
  @GET
  @Path("/{id}/replicaHistory")
  public Response getReplicaHistoryForContainer(
      @PathParam("id") Long containerID) {
    return Response.ok(
        containerManager.getAllContainerHistory(containerID)).build();
  }

  /**
   * Return
   * {@link org.apache.hadoop.ozone.recon.api.types.MissingContainerMetadata}
   * for all missing containers.
   *
   * @return {@link Response}
   */
  @GET
  @Path("/missing")
  public Response getMissingContainers() {
    List<MissingContainerMetadata> missingContainers = new ArrayList<>();
    containerHealthSchemaManager.getUnhealthyContainers(
        UnHealthyContainerStates.MISSING, 0, Integer.MAX_VALUE)
        .forEach(container -> {
          long containerID = container.getContainerId();
          try {
            ContainerInfo containerInfo =
                containerManager.getContainer(new ContainerID(containerID));
            long keyCount = containerInfo.getNumberOfKeys();
            UUID pipelineID = containerInfo.getPipelineID().getId();

            List<ContainerHistory> datanodes =
                containerManager.getLatestContainerHistory(containerID,
                    containerInfo.getReplicationFactor().getNumber());
            missingContainers.add(new MissingContainerMetadata(containerID,
                container.getInStateSince(), keyCount, pipelineID, datanodes));
          } catch (IOException ioEx) {
            throw new WebApplicationException(ioEx,
                Response.Status.INTERNAL_SERVER_ERROR);
          }
        });
    MissingContainersResponse response =
        new MissingContainersResponse(missingContainers.size(),
            missingContainers);
    return Response.ok(response).build();
  }

  /**
   * Return
   * {@link org.apache.hadoop.ozone.recon.api.types.UnhealthyContainerMetadata}
   * for all unhealthy containers.
   *
   * @param state Return only containers matching the given unhealthy state,
   *              eg UNDER_REPLICATED, MIS_REPLICATED, OVER_REPLICATED or
   *              MISSING. Passing null returns all containers.
   * @param limit The limit of unhealthy containers to return.
   * @param batchNum The batch number (like "page number") of results to return.
   *                 Passing 1, will return records 1 to limit. 2 will return
   *                 limit + 1 to 2 * limit, etc.
   * @return {@link Response}
   */
  @GET
  @Path("/unhealthy/{state}")
  public Response getUnhealthyContainers(
      @PathParam("state") String state,
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
          int limit,
      @DefaultValue(DEFAULT_BATCH_NUMBER)
      @QueryParam(RECON_QUERY_BATCH_PARAM) int batchNum) {
    int offset = Math.max(((batchNum - 1) * limit), 0);

    List<UnhealthyContainerMetadata> unhealthyMeta = new ArrayList<>();
    List<UnhealthyContainersSummary> summary;
    try {
      UnHealthyContainerStates internalState = null;

      if (state != null) {
        // If an invalid state is passed in, this will throw
        // illegalArgumentException and fail the request
        internalState = UnHealthyContainerStates.valueOf(state);
      }

      summary = containerHealthSchemaManager.getUnhealthyContainersSummary();
      List<UnhealthyContainers> containers = containerHealthSchemaManager
          .getUnhealthyContainers(internalState, offset, limit);
      for (UnhealthyContainers c : containers) {
        long containerID = c.getContainerId();
        ContainerInfo containerInfo =
            containerManager.getContainer(new ContainerID(containerID));
        long keyCount = containerInfo.getNumberOfKeys();
        UUID pipelineID = containerInfo.getPipelineID().getId();
        List<ContainerHistory> datanodes =
            containerManager.getLatestContainerHistory(containerID,
                containerInfo.getReplicationFactor().getNumber());
        unhealthyMeta.add(new UnhealthyContainerMetadata(
            c, datanodes, pipelineID, keyCount));
      }
    } catch (IOException ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    }

    UnhealthyContainersResponse response =
        new UnhealthyContainersResponse(unhealthyMeta);
    for (UnhealthyContainersSummary s : summary) {
      response.setSummaryCount(s.getContainerState(), s.getCount());
    }
    return Response.ok(response).build();
  }

  /**
   * Return
   * {@link org.apache.hadoop.ozone.recon.api.types.UnhealthyContainerMetadata}
   * for all unhealthy containers.

   * @param limit The limit of unhealthy containers to return.
   * @param batchNum The batch number (like "page number") of results to return.
   *                 Passing 1, will return records 1 to limit. 2 will return
   *                 limit + 1 to 2 * limit, etc.
   * @return {@link Response}
   */
  @GET
  @Path("/unhealthy")
  public Response getUnhealthyContainers(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
          int limit,
      @DefaultValue(DEFAULT_BATCH_NUMBER)
      @QueryParam(RECON_QUERY_BATCH_PARAM) int batchNum) {
    return getUnhealthyContainers(null, limit, batchNum);
  }

  /**
   * Helper function to extract the blocks for a given container from a given
   * OM Key.
   * @param matchedKeys List of OM Key Info locations
   * @param containerID containerId.
   * @return List of blocks.
   */
  private List<ContainerBlockMetadata> getBlocks(
      List<OmKeyLocationInfoGroup> matchedKeys, long containerID) {
    List<ContainerBlockMetadata> blockIds = new ArrayList<>();
    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : matchedKeys) {
      List<OmKeyLocationInfo> omKeyLocationInfos = omKeyLocationInfoGroup
          .getLocationList()
          .stream()
          .filter(c -> c.getContainerID() == containerID)
          .collect(Collectors.toList());
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfos) {
        blockIds.add(new ContainerBlockMetadata(omKeyLocationInfo
            .getContainerID(), omKeyLocationInfo.getLocalID()));
      }
    }
    return blockIds;
  }
}

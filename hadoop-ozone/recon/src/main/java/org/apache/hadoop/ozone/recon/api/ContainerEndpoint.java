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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.api.types.ContainerDiscrepancyInfo;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.ContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.DeletedContainerInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata.ContainerBlockMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeysResponse;
import org.apache.hadoop.ozone.recon.api.types.MissingContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.MissingContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersSummary;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.persistence.ContainerHistory;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.hadoop.ozone.recon.schema.tables.pojos.UnhealthyContainers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FILTER_FOR_MISSING_CONTAINERS;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_FILTER;
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
@AdminOnly
public class ContainerEndpoint {

  @Inject
  private ReconContainerMetadataManager reconContainerMetadataManager;

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  private final ReconContainerManager containerManager;
  private final PipelineManager pipelineManager;
  private final ContainerHealthSchemaManager containerHealthSchemaManager;
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final OzoneStorageContainerManager reconSCM;
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerEndpoint.class);
  private BucketLayout layout = BucketLayout.DEFAULT;

  /**
   * Enumeration representing different data filters.
   * Each filter has an associated value.
   */
  public enum DataFilter {
    SCM("SCM"),  // Filter for SCM
    OM("OM");    // Filter for OM

    private final String value;

    DataFilter(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    /**
     * Convert a String value to the corresponding DataFilter enum constant.
     * The comparison is case-insensitive.
     */
    public static DataFilter fromValue(String value) {
      for (DataFilter filter : DataFilter.values()) {
        if (filter.getValue().equalsIgnoreCase(value)) {
          return filter;
        }
      }
      throw new IllegalArgumentException("Invalid DataFilter value: " + value);
    }
  }


  @Inject
  public ContainerEndpoint(OzoneStorageContainerManager reconSCM,
               ContainerHealthSchemaManager containerHealthSchemaManager,
               ReconNamespaceSummaryManager reconNamespaceSummaryManager) {
    this.containerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.pipelineManager = reconSCM.getPipelineManager();
    this.containerHealthSchemaManager = containerHealthSchemaManager;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconSCM = reconSCM;
  }

  /**
   * Return @{@link org.apache.hadoop.hdds.scm.container}
   * for the containers starting from the given "prev-key" query param for the
   * given "limit". The given "prev-key" is skipped from the results returned.
   * @param prevKey the containerID after which results are returned.
   *                start containerID, >=0,
   *                start searching at the head if 0.
   * @param limit max no. of containers to get.
   *              count must be >= 0
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big.
   * @return {@link Response}
   */
  @GET
  public Response getContainers(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
          int limit,
      @DefaultValue(PREV_CONTAINER_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_PREVKEY) long prevKey) {
    if (limit < 0 || prevKey < 0) {
      // Send back an empty response
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }

    long containersCount;
    List<ContainerMetadata> containerMetaDataList =
        // Get the containers starting from the prevKey+1 which will skip the
        // container having prevKey ID
        containerManager.getContainers(ContainerID.valueOf(prevKey + 1), limit)
            .stream()
            .map(container -> {
              ContainerMetadata containerMetadata =
                  new ContainerMetadata(container.getContainerID());
              containerMetadata.setNumberOfKeys(container.getNumberOfKeys());
              return containerMetadata;
            })
            .collect(Collectors.toList());

    containersCount = containerMetaDataList.size();

    // Get the last container ID from the List
    long lastContainerID = containerMetaDataList.isEmpty() ? prevKey :
        containerMetaDataList.get(containerMetaDataList.size() - 1)
            .getContainerID();

    ContainersResponse containersResponse =
        new ContainersResponse(containersCount, containerMetaDataList,
            lastContainerID);
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
          reconContainerMetadataManager.getKeyPrefixesForContainer(containerID,
              prevKeyPrefix);
      // Get set of Container-Key mappings for given containerId.
      for (ContainerKeyPrefix containerKeyPrefix : containerKeyPrefixMap
          .keySet()) {

        // Directly calling getSkipCache() on the Key/FileTable table
        // instead of iterating since only full keys are supported now. We will
        // try to get the OmKeyInfo object by searching the KEY_TABLE table with
        // the key prefix. If it's not found, we will then search the FILE_TABLE
        OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(BucketLayout.LEGACY)
            .getSkipCache(containerKeyPrefix.getKeyPrefix());
        if (omKeyInfo == null) {
          omKeyInfo =
              omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
                  .getSkipCache(containerKeyPrefix.getKeyPrefix());
        }

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
          reconContainerMetadataManager.getKeyCountForContainer(containerID);
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
   * @param limit The limit of missing containers to return.
   * @return {@link Response}
   */
  @GET
  @Path("/missing")
  @Deprecated
  public Response getMissingContainers(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit
  ) {
    List<MissingContainerMetadata> missingContainers = new ArrayList<>();
    containerHealthSchemaManager.getUnhealthyContainers(
        UnHealthyContainerStates.MISSING, 0, limit)
        .forEach(container -> {
          long containerID = container.getContainerId();
          try {
            ContainerInfo containerInfo =
                containerManager.getContainer(ContainerID.valueOf(containerID));
            long keyCount = containerInfo.getNumberOfKeys();
            UUID pipelineID = containerInfo.getPipelineID().getId();

            List<ContainerHistory> datanodes =
                containerManager.getLatestContainerHistory(containerID,
                    containerInfo.getReplicationConfig().getRequiredNodes());
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
      containers.stream()
          .filter(
              container -> !container.getContainerState().equals(UnHealthyContainerStates.EMPTY_MISSING.toString()));
      for (UnhealthyContainers c : containers) {
        long containerID = c.getContainerId();
        ContainerInfo containerInfo =
            containerManager.getContainer(ContainerID.valueOf(containerID));
        long keyCount = containerInfo.getNumberOfKeys();
        UUID pipelineID = containerInfo.getPipelineID().getId();
        List<ContainerHistory> datanodes =
            containerManager.getLatestContainerHistory(containerID,
                containerInfo.getReplicationConfig().getRequiredNodes());
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
   * This API will return all DELETED containers in SCM in below JSON format.
   * {
   * containers: [
   * {
   *  containerId: 1,
   *  state: DELETED,
   *  pipelineId: "a10ffab6-8ed5-414a-aaf5-79890ff3e8a1",
   *  numOfKeys: 3,
   *  inStateSince: <stateEnterTime>
   * },
   * {
   *  containerId: 2,
   *  state: DELETED,
   *  pipelineId: "a10ffab6-8ed5-414a-aaf5-79890ff3e8a1",
   *  numOfKeys: 6,
   *  inStateSince: <stateEnterTime>
   * }
   * ]
   * }
   * @param limit limits the number of deleted containers
   * @param prevKey previous container Id to skip
   * @return Response of deleted containers.
   */
  @GET
  @Path("/deleted")
  public Response getSCMDeletedContainers(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(PREV_CONTAINER_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_PREVKEY) long prevKey) {
    List<DeletedContainerInfo> deletedContainerInfoList = new ArrayList<>();
    try {
      List<ContainerInfo> containers =
          containerManager.getContainers(ContainerID.valueOf(prevKey), limit,
              HddsProtos.LifeCycleState.DELETED);
      containers = containers.stream()
          .filter(containerInfo -> !(containerInfo.getContainerID() == prevKey))
          .collect(
              Collectors.toList());
      containers.forEach(containerInfo -> {
        DeletedContainerInfo deletedContainerInfo = new DeletedContainerInfo();
        deletedContainerInfo.setContainerID(containerInfo.getContainerID());
        deletedContainerInfo.setPipelineID(containerInfo.getPipelineID());
        deletedContainerInfo.setNumberOfKeys(containerInfo.getNumberOfKeys());
        deletedContainerInfo.setContainerState(containerInfo.getState().name());
        deletedContainerInfo.setStateEnterTime(
            containerInfo.getStateEnterTime().toEpochMilli());
        deletedContainerInfo.setLastUsed(
            containerInfo.getLastUsed().toEpochMilli());
        deletedContainerInfo.setUsedBytes(containerInfo.getUsedBytes());
        deletedContainerInfo.setReplicationConfig(
            containerInfo.getReplicationConfig());
        deletedContainerInfo.setReplicationFactor(
            containerInfo.getReplicationFactor().name());
        deletedContainerInfoList.add(deletedContainerInfo);
      });
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.ok(deletedContainerInfoList).build();
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

  /**
   * Retrieves the container mismatch insights.
   *
   * This method returns a list of ContainerDiscrepancyInfo objects representing
   * the containers that are missing in either the Ozone Manager (OM) or the
   * Storage Container Manager (SCM), based on the provided filter parameter.
   * The returned list is paginated based on the provided limit and prevKey
   * parameters.
   *
   * @param limit   The maximum number of container discrepancies to return.
   * @param prevKey The container ID after which the results are returned.
   * @param missingIn  The missing filter parameter to specify if it's
   *                   "OM" or "SCM" missing containers to be returned.
   */
  @GET
  @Path("/mismatch")
  public Response getContainerMisMatchInsights(
      @DefaultValue(DEFAULT_FETCH_COUNT)
      @QueryParam(RECON_QUERY_LIMIT) int limit,
      @DefaultValue(PREV_CONTAINER_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_PREVKEY) long prevKey,
      @DefaultValue(DEFAULT_FILTER_FOR_MISSING_CONTAINERS)
      @QueryParam(RECON_QUERY_FILTER) String missingIn) {
    if (prevKey < 0 || limit < 0) {
      // Send back an empty response
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }

    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList =
        new ArrayList<>();
    try {
      Map<Long, ContainerMetadata> omContainers =
          reconContainerMetadataManager.getContainers(-1, -1);
      List<Long> scmNonDeletedContainers =
          containerManager.getContainers().stream()
              .filter(containerInfo -> containerInfo.getState() !=
                  HddsProtos.LifeCycleState.DELETED)
              .map(containerInfo -> containerInfo.getContainerID())
              .collect(Collectors.toList());
      DataFilter dataFilter = DataFilter.fromValue(missingIn.toUpperCase());

      switch (dataFilter) {

      case SCM:
        List<Map.Entry<Long, ContainerMetadata>> notSCMContainers =
            omContainers.entrySet().stream()
                .filter(
                    containerMetadataEntry -> !scmNonDeletedContainers.contains(
                        containerMetadataEntry.getKey()))
                .collect(Collectors.toList());

        if (prevKey > 0) {
          int index = 0;
          while (index < notSCMContainers.size() &&
              notSCMContainers.get(index).getKey() <= prevKey) {
            index++;
          }
          if (index < notSCMContainers.size()) {
            notSCMContainers = notSCMContainers.subList(index,
                Math.min(index + limit, notSCMContainers.size()));
          } else {
            notSCMContainers = Collections.emptyList();
          }
        } else {
          notSCMContainers = notSCMContainers.subList(0,
              Math.min(limit, notSCMContainers.size()));
        }

        notSCMContainers.forEach(nonSCMContainer -> {
          ContainerDiscrepancyInfo containerDiscrepancyInfo =
              new ContainerDiscrepancyInfo();
          containerDiscrepancyInfo.setContainerID(nonSCMContainer.getKey());
          containerDiscrepancyInfo.setNumberOfKeys(
              nonSCMContainer.getValue().getNumberOfKeys());
          containerDiscrepancyInfo.setPipelines(
              nonSCMContainer.getValue().getPipelines());
          containerDiscrepancyInfo.setExistsAt("OM");
          containerDiscrepancyInfoList.add(containerDiscrepancyInfo);
        });
        break;

      case OM:
        List<Long> nonOMContainers = scmNonDeletedContainers.stream()
            .filter(containerId -> !omContainers.containsKey(containerId))
            .collect(Collectors.toList());

        if (prevKey > 0) {
          int index = 0;
          while (index < nonOMContainers.size() &&
              nonOMContainers.get(index) <= prevKey) {
            index++;
          }
          if (index < nonOMContainers.size()) {
            nonOMContainers = nonOMContainers.subList(index,
                Math.min(index + limit, nonOMContainers.size()));
          } else {
            nonOMContainers = Collections.emptyList();
          }
        } else {
          nonOMContainers = nonOMContainers.subList(0,
              Math.min(limit, nonOMContainers.size()));
        }

        List<Pipeline> pipelines = new ArrayList<>();
        nonOMContainers.forEach(nonOMContainerId -> {
          boolean containerExistsInScm = true;
          ContainerDiscrepancyInfo containerDiscrepancyInfo =
              new ContainerDiscrepancyInfo();
          containerDiscrepancyInfo.setContainerID(nonOMContainerId);
          containerDiscrepancyInfo.setNumberOfKeys(0);
          PipelineID pipelineID = null;
          try {
            pipelineID = containerManager.getContainer(
                ContainerID.valueOf(nonOMContainerId)).getPipelineID();
            if (pipelineID != null) {
              pipelines.add(pipelineManager.getPipeline(pipelineID));
            }
          } catch (ContainerNotFoundException e) {
            containerExistsInScm = false;
            LOG.warn("Container {} not found in SCM: {}", nonOMContainerId,
                e);
          } catch (PipelineNotFoundException e) {
            LOG.debug(
                "Pipeline not found for container: {} and pipelineId: {}",
                nonOMContainerId, pipelineID, e);
          }
          // The container might have been deleted in SCM after the call to
          // get the list of containers
          if (containerExistsInScm) {
            containerDiscrepancyInfo.setPipelines(pipelines);
            containerDiscrepancyInfo.setExistsAt("SCM");
            containerDiscrepancyInfoList.add(containerDiscrepancyInfo);
          }
        });
        break;

      default:
        // Invalid filter parameter value
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
    } catch (IOException ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    Map<String, Object> response = new HashMap<>();
    if (!containerDiscrepancyInfoList.isEmpty()) {
      response.put("lastKey", containerDiscrepancyInfoList.get(
          containerDiscrepancyInfoList.size() - 1).getContainerID());
    } else {
      response.put("lastKey", null);
    }
    response.put("containerDiscrepancyInfo", containerDiscrepancyInfoList);


    return Response.ok(response).build();
  }


  /** This API retrieves set of deleted containers in SCM which are present
   * in OM to find out list of keys mapped to such DELETED state containers.
   *
   * limit - limits the number of such SCM DELETED containers present in OM.
   * prevKey - Skip containers till it seeks correctly to the previous
   * containerId.
   * Sample API Response:
   * [
   *   {
   *     "containerId": 2,
   *     "numberOfKeys": 2,
   *     "pipelines": []
   *   }
   * ]
   */
  @GET
  @Path("/mismatch/deleted")
  public Response getOmContainersDeletedInSCM(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(PREV_CONTAINER_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_PREVKEY) long prevKey) {
    if (prevKey < 0) {
      // Send back an empty response
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }
    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList =
        new ArrayList<>();
    try {
      Map<Long, ContainerMetadata> omContainers =
          reconContainerMetadataManager.getContainers(limit, prevKey);

      List<Long> deletedStateSCMContainerIds =
          containerManager.getContainers().stream()
              .filter(containerInfo -> (containerInfo.getState() ==
                  HddsProtos.LifeCycleState.DELETED))
              .map(containerInfo -> containerInfo.getContainerID()).collect(
                  Collectors.toList());

      List<Map.Entry<Long, ContainerMetadata>>
          omContainersDeletedInSCM =
          omContainers.entrySet().stream().filter(containerMetadataEntry ->
                  (deletedStateSCMContainerIds.contains(
                      containerMetadataEntry.getKey())))
              .collect(
                  Collectors.toList());

      omContainersDeletedInSCM.forEach(
          containerMetadataEntry -> {
            ContainerDiscrepancyInfo containerDiscrepancyInfo =
                new ContainerDiscrepancyInfo();
            containerDiscrepancyInfo.setContainerID(
                containerMetadataEntry.getKey());
            containerDiscrepancyInfo.setNumberOfKeys(
                containerMetadataEntry.getValue().getNumberOfKeys());
            containerDiscrepancyInfo.setPipelines(
                containerMetadataEntry.getValue()
                    .getPipelines());
            containerDiscrepancyInfoList.add(containerDiscrepancyInfo);
          });
    } catch (IOException ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    Map<String, Object> response = new HashMap<>();
    if (!containerDiscrepancyInfoList.isEmpty()) {
      response.put("lastKey", containerDiscrepancyInfoList.get(
          containerDiscrepancyInfoList.size() - 1).getContainerID());
    } else {
      response.put("lastKey", null);
    }
    response.put("containerDiscrepancyInfo", containerDiscrepancyInfoList);
    return Response.ok(response).build();
  }
}

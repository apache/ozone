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

import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FILTER_FOR_MISSING_CONTAINERS;
import static org.apache.hadoop.ozone.recon.ReconConstants.PREV_CONTAINER_ID_DEFAULT_VALUE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_FILTER;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_MAX_CONTAINER_ID;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_MIN_CONTAINER_ID;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.ReconUtils;
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
import org.apache.hadoop.ozone.util.SeekableIterator;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Endpoint for querying keys that belong to a container.
 */
@Path("/containers")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class ContainerEndpoint {

  private ReconContainerMetadataManager reconContainerMetadataManager;
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
                           ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                           ReconContainerMetadataManager reconContainerMetadataManager,
                           ReconOMMetadataManager omMetadataManager) {
    this.containerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.pipelineManager = reconSCM.getPipelineManager();
    this.containerHealthSchemaManager = containerHealthSchemaManager;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconSCM = reconSCM;
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.omMetadataManager = omMetadataManager;
  }

  /**
   * Return {@code org.apache.hadoop.hdds.scm.container}
   * for the containers starting from the given "prev-key" query param for the
   * given "limit". The given "prev-key" is skipped from the results returned.
   *
   * @param prevKey the containerID after which results are returned.
   *                start containerID, &gt;=0,
   *                start searching at the head if 0.
   * @param limit   max no. of containers to get.
   *                count must be &gt;= 0
   *                Usually the count will be replace with a very big
   *                value instead of being unlimited in case the db is very big.
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
   * @param containerID   the given containerID.
   * @param limit         max no. of keys to get.
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

    // Total count of keys in the container.
    long totalCount;
    // Last key prefix to be used for pagination. It will be exposed in the response.
    String lastKey = "";

    // If -1 is passed, set limit to the maximum integer value to retrieve all records
    if (limit == -1) {
      limit = Integer.MAX_VALUE;
    }

    try {
      Map<ContainerKeyPrefix, Integer> containerKeyPrefixMap =
          reconContainerMetadataManager.getKeyPrefixesForContainer(containerID, prevKeyPrefix, limit);
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

          String ozoneKey = containerKeyPrefix.getKeyPrefix();
          lastKey = ozoneKey;
          if (keyMetadataMap.containsKey(ozoneKey)) {
            keyMetadataMap.get(ozoneKey).getVersions()
                .add(containerKeyPrefix.getKeyVersion());

            keyMetadataMap.get(ozoneKey).getBlockIds()
                .put(containerKeyPrefix.getKeyVersion(), blockIds);
          } else {
            KeyMetadata keyMetadata = new KeyMetadata();
            keyMetadata.setBucket(omKeyInfo.getBucketName());
            keyMetadata.setVolume(omKeyInfo.getVolumeName());
            keyMetadata.setKey(omKeyInfo.getKeyName());
            keyMetadata.setCompletePath(ReconUtils.constructFullPath(omKeyInfo,
                reconNamespaceSummaryManager));
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
      throw new WebApplicationException(ioEx, Response.Status.INTERNAL_SERVER_ERROR);
    }
    KeysResponse keysResponse = new KeysResponse(totalCount, keyMetadataMap.values(), lastKey);
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
            UnHealthyContainerStates.MISSING, 0L, Optional.empty(), limit)
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
   * @param maxContainerId Upper bound for container IDs to include (exclusive).
   *                       When specified, returns containers with IDs less than this value
   *                       in descending order. Use for backward pagination.
   * @param minContainerId Lower bound for container IDs to include (exclusive).
   *                       When maxContainerId is not specified, returns containers with IDs
   *                       greater than this value in ascending order. Use for forward pagination.
   * @return {@link Response}
   */
  @GET
  @Path("/unhealthy/{state}")
  public Response getUnhealthyContainers(
      @PathParam("state") String state,
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(PREV_CONTAINER_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_MAX_CONTAINER_ID) long maxContainerId,
      @DefaultValue(PREV_CONTAINER_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_MIN_CONTAINER_ID) long minContainerId) {
    Optional<Long> maxContainerIdOpt = maxContainerId > 0 ? Optional.of(maxContainerId) : Optional.empty();
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
          .getUnhealthyContainers(internalState, minContainerId, maxContainerIdOpt, limit);

      // Filtering out EMPTY_MISSING and NEGATIVE_SIZE containers from the response.
      // These container states are not being inserted into the database as they represent
      // edge cases that are not critical to track as unhealthy containers.
      List<UnhealthyContainers> filteredContainers = containers.stream()
          .filter(container -> !container.getContainerState()
              .equals(UnHealthyContainerStates.EMPTY_MISSING.toString())
              && !container.getContainerState()
              .equals(UnHealthyContainerStates.NEGATIVE_SIZE.toString()))
          .collect(Collectors.toList());

      for (UnhealthyContainers c : filteredContainers) {
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
    if (!unhealthyMeta.isEmpty()) {
      response.setFirstKey(unhealthyMeta.stream().map(UnhealthyContainerMetadata::getContainerID)
          .min(Long::compareTo).orElse(0L));
      response.setLastKey(unhealthyMeta.stream().map(UnhealthyContainerMetadata::getContainerID)
          .max(Long::compareTo).orElse(0L));
    }
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
   * @param maxContainerId Upper bound for container IDs to include (exclusive).
   *                       When specified, returns containers with IDs less than this value
   *                       in descending order. Use for backward pagination.
   * @param minContainerId Lower bound for container IDs to include (exclusive).
   *                       When maxContainerId is not specified, returns containers with IDs
   *                       greater than this value in ascending order. Use for forward pagination.
   * @return {@link Response}
   */
  @GET
  @Path("/unhealthy")
  public Response getUnhealthyContainers(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(PREV_CONTAINER_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_MAX_CONTAINER_ID) long maxContainerId,
      @DefaultValue(PREV_CONTAINER_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_MIN_CONTAINER_ID) long minContainerId) {
    return getUnhealthyContainers(null, limit, maxContainerId, minContainerId);
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
   *
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
    Long minContainerID = prevKey + 1;
    Iterator<ContainerInfo> scmNonDeletedContainers =
            containerManager.getContainers().stream()
                    .filter(containerInfo -> (containerInfo.getContainerID() >= minContainerID))
                    .filter(containerInfo -> containerInfo.getState() != HddsProtos.LifeCycleState.DELETED)
                    .sorted(Comparator.comparingLong(ContainerInfo::getContainerID)).iterator();
    ContainerInfo scmContainerInfo = scmNonDeletedContainers.hasNext() ?
            scmNonDeletedContainers.next() : null;
    DataFilter dataFilter = DataFilter.fromValue(missingIn.toUpperCase());
    try (SeekableIterator<Long, ContainerMetadata> omContainers =
                 reconContainerMetadataManager.getContainersIterator()) {
      omContainers.seek(minContainerID);
      ContainerMetadata containerMetadata = omContainers.hasNext() ? omContainers.next() : null;
      switch (dataFilter) {
      case SCM:
        List<ContainerMetadata> notSCMContainers = new ArrayList<>();
        while (containerMetadata != null && notSCMContainers.size() < limit) {
          Long omContainerID = containerMetadata.getContainerID();
          Long scmContainerID = scmContainerInfo == null ? null : scmContainerInfo.getContainerID();
          if (omContainerID.equals(scmContainerID)) {
            containerMetadata = omContainers.hasNext() ? omContainers.next() : null;
            scmContainerInfo = scmNonDeletedContainers.hasNext() ? scmNonDeletedContainers.next() : null;
          } else if (scmContainerID == null || omContainerID.compareTo(scmContainerID) < 0) {
            notSCMContainers.add(containerMetadata);
            containerMetadata = omContainers.hasNext() ? omContainers.next() : null;
          } else {
            scmContainerInfo = scmNonDeletedContainers.hasNext() ? scmNonDeletedContainers.next() : null;
          }
        }

        notSCMContainers.forEach(nonSCMContainer -> {
          ContainerDiscrepancyInfo containerDiscrepancyInfo =
              new ContainerDiscrepancyInfo();
          containerDiscrepancyInfo.setContainerID(nonSCMContainer.getContainerID());
          containerDiscrepancyInfo.setNumberOfKeys(
              nonSCMContainer.getNumberOfKeys());
          containerDiscrepancyInfo.setPipelines(
              nonSCMContainer.getPipelines());
          containerDiscrepancyInfo.setExistsAt("OM");
          containerDiscrepancyInfoList.add(containerDiscrepancyInfo);
        });
        break;

      case OM:
        List<ContainerInfo> nonOMContainers = new ArrayList<>();
        while (scmContainerInfo != null && nonOMContainers.size() < limit) {
          Long omContainerID = containerMetadata == null ? null : containerMetadata.getContainerID();
          Long scmContainerID = scmContainerInfo.getContainerID();
          if (scmContainerID.equals(omContainerID)) {
            scmContainerInfo = scmNonDeletedContainers.hasNext() ? scmNonDeletedContainers.next() : null;
            containerMetadata = omContainers.hasNext() ? omContainers.next() : null;
          } else if (omContainerID == null || scmContainerID.compareTo(omContainerID) < 0) {
            nonOMContainers.add(scmContainerInfo);
            scmContainerInfo = scmNonDeletedContainers.hasNext() ? scmNonDeletedContainers.next() : null;
          } else {
            //Seeking directly to SCM containerId sequential read is just wasteful here if there are too many values
            // to be read in b/w omContainerID & scmContainerID since (omContainerId<scmContainerID)
            omContainers.seek(scmContainerID);
            containerMetadata = omContainers.hasNext() ? omContainers.next() : null;
          }
        }

        nonOMContainers.forEach(containerInfo -> {
          ContainerDiscrepancyInfo containerDiscrepancyInfo = new ContainerDiscrepancyInfo();
          containerDiscrepancyInfo.setContainerID(containerInfo.getContainerID());
          containerDiscrepancyInfo.setNumberOfKeys(0);
          List<Pipeline> pipelines = new ArrayList<>();
          PipelineID pipelineID = null;
          try {
            pipelineID = containerInfo.getPipelineID();
            if (pipelineID != null) {
              pipelines.add(pipelineManager.getPipeline(pipelineID));
            }
          } catch (PipelineNotFoundException e) {
            LOG.debug(
                "Pipeline not found for container: {} and pipelineId: {}",
                containerInfo, pipelineID, e);
          }
          containerDiscrepancyInfo.setPipelines(pipelines);
          containerDiscrepancyInfo.setExistsAt("SCM");
          containerDiscrepancyInfoList.add(containerDiscrepancyInfo);
        });
        break;

      default:
        // Invalid filter parameter value
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
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


  /**
   * This API retrieves set of deleted containers in SCM which are present
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
    if (limit <= 0) {
      limit = Integer.MAX_VALUE;
    }
    long minContainerID = prevKey + 1;
    Iterator<ContainerInfo> deletedStateSCMContainers = containerManager.getContainers().stream()
        .filter(containerInfo -> containerInfo.getContainerID() >= minContainerID)
        .filter(containerInfo -> containerInfo.getState() == HddsProtos.LifeCycleState.DELETED)
        .sorted(Comparator.comparingLong(ContainerInfo::getContainerID)).iterator();
    List<ContainerDiscrepancyInfo> containerDiscrepancyInfoList;
    try (SeekableIterator<Long, ContainerMetadata> omContainers =
           reconContainerMetadataManager.getContainersIterator()) {
      ContainerInfo scmContainerInfo = deletedStateSCMContainers.hasNext() ? deletedStateSCMContainers.next() : null;
      ContainerMetadata containerMetadata = omContainers.hasNext() ? omContainers.next() : null;
      List<ContainerMetadata> omContainersDeletedInSCM = new ArrayList<>();
      while (containerMetadata != null && scmContainerInfo != null
        && omContainersDeletedInSCM.size() < limit) {
        Long omContainerID = containerMetadata.getContainerID();
        Long scmContainerID = scmContainerInfo.getContainerID();
        if (scmContainerID.equals(omContainerID)) {
          omContainersDeletedInSCM.add(containerMetadata);
          scmContainerInfo = deletedStateSCMContainers.hasNext() ? deletedStateSCMContainers.next() : null;
          containerMetadata = omContainers.hasNext() ? omContainers.next() : null;
        } else if (scmContainerID.compareTo(omContainerID) < 0) {
          scmContainerInfo = deletedStateSCMContainers.hasNext() ? deletedStateSCMContainers.next() : null;
        } else {
          // Seek directly to scmContainerId iterating sequentially is very wasteful here.
          omContainers.seek(scmContainerID);
          containerMetadata = omContainers.hasNext() ? omContainers.next() : null;
        }
      }

      containerDiscrepancyInfoList = omContainersDeletedInSCM.stream().map(containerMetadataEntry -> {
        ContainerDiscrepancyInfo containerDiscrepancyInfo = new ContainerDiscrepancyInfo();
        containerDiscrepancyInfo.setContainerID(containerMetadataEntry.getContainerID());
        containerDiscrepancyInfo.setNumberOfKeys(containerMetadataEntry.getNumberOfKeys());
        containerDiscrepancyInfo.setPipelines(containerMetadataEntry.getPipelines());
        return containerDiscrepancyInfo;
      }).collect(Collectors.toList());
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

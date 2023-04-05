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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeyEntityInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResp;
import org.apache.hadoop.ozone.recon.api.types.KeysResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;

/**
 * Endpoint to get following key level info under OM DB Insight page of Recon.
 * 1. Number of open keys for Legacy/OBS buckets.
 * 2. Number of open files for FSO buckets.
 * 3. Amount of data mapped to open keys and open files.
 * 4. Number of pending delete keys in legacy/OBS buckets and pending
 * delete files in FSO buckets.
 * 5. Amount of data mapped to pending delete keys in legacy/OBS buckets and
 * pending delete files in FSO buckets.
 */
@Path("/omdbinsight")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class OMDBInsightEndpoint {

  @Inject
  private ContainerEndpoint containerEndpoint;
  @Inject
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private final ReconOMMetadataManager omMetadataManager;
  private final ReconContainerManager containerManager;

  @Inject
  public OMDBInsightEndpoint(OzoneStorageContainerManager reconSCM,
                             ReconOMMetadataManager omMetadataManager) {
    this.containerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.omMetadataManager = omMetadataManager;
  }

  /**
   * This method retrieves set of keys/files which are open.
   *
   * @return the http json response wrapped in below format:
   * {
   *     replicatedTotal: 13824,
   *     unreplicatedTotal: 4608,
   *     entities: [
   *     {
   *         path: “/vol1/bucket1/key1”,
   *         keyState: “Open”,
   *         inStateSince: 1667564193026,
   *         size: 1024,
   *         replicatedSize: 3072,
   *         unreplicatedSize: 1024,
   *         replicationType: RATIS,
   *         replicationFactor: THREE
   *     }.
   *    {
   *         path: “/vol1/bucket1/key2”,
   *         keyState: “Open”,
   *         inStateSince: 1667564193026,
   *         size: 512,
   *         replicatedSize: 1536,
   *         unreplicatedSize: 512,
   *         replicationType: RATIS,
   *         replicationFactor: THREE
   *     }.
   *     {
   *         path: “/vol1/fso-bucket/dir1/file1”,
   *         keyState: “Open”,
   *         inStateSince: 1667564193026,
   *         size: 1024,
   *         replicatedSize: 3072,
   *         unreplicatedSize: 1024,
   *         replicationType: RATIS,
   *         replicationFactor: THREE
   *     }.
   *     {
   *         path: “/vol1/fso-bucket/dir1/dir2/file2”,
   *         keyState: “Open”,
   *         inStateSince: 1667564193026,
   *         size: 2048,
   *         replicatedSize: 6144,
   *         unreplicatedSize: 2048,
   *         replicationType: RATIS,
   *         replicationFactor: THREE
   *     }
   *   ]
   * }
   */
  @GET
  @Path("openkeyinfo")
  public Response getOpenKeyInfo(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
      String prevKeyPrefix) {
    KeyInsightInfoResp openKeyInsightInfo = new KeyInsightInfoResp();
    List<KeyEntityInfo> nonFSOKeyInfoList =
        openKeyInsightInfo.getNonFSOKeyInfoList();
    boolean isLegacyBucketLayout = true;
    boolean recordsFetchedLimitReached = false;
    List<KeyEntityInfo> fsoKeyInfoList = openKeyInsightInfo.getFsoKeyInfoList();
    for (BucketLayout layout : Arrays.asList(BucketLayout.LEGACY,
        BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
      isLegacyBucketLayout = (layout == BucketLayout.LEGACY);
      Table<String, OmKeyInfo> openKeyTable =
          omMetadataManager.getOpenKeyTable(layout);
      try (
          TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
              keyIter = openKeyTable.iterator()) {
        boolean skipPrevKey = false;
        String seekKey = prevKeyPrefix;
        if (StringUtils.isNotBlank(prevKeyPrefix)) {
          skipPrevKey = true;
          Table.KeyValue<String, OmKeyInfo> seekKeyValue =
              keyIter.seek(seekKey);
          // check if RocksDB was able to seek correctly to the given key prefix
          // if not, then return empty result
          // In case of an empty prevKeyPrefix, all the keys are returned
          if (seekKeyValue == null ||
              (StringUtils.isNotBlank(prevKeyPrefix) &&
                  !seekKeyValue.getKey().equals(prevKeyPrefix))) {
            return Response.ok(openKeyInsightInfo).build();
          }
        }
        while (keyIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
          String key = kv.getKey();
          OmKeyInfo omKeyInfo = kv.getValue();
          // skip the prev key if prev key is present
          if (skipPrevKey && key.equals(prevKeyPrefix)) {
            continue;
          }
          KeyEntityInfo keyEntityInfo = new KeyEntityInfo();
          keyEntityInfo.setKey(key);
          keyEntityInfo.setPath(omKeyInfo.getKeyName());
          keyEntityInfo.setInStateSince(omKeyInfo.getCreationTime());
          keyEntityInfo.setSize(omKeyInfo.getDataSize());
          keyEntityInfo.setReplicatedSize(omKeyInfo.getReplicatedSize());
          keyEntityInfo.setReplicationConfig(omKeyInfo.getReplicationConfig());
          openKeyInsightInfo.setUnreplicatedTotal(
              openKeyInsightInfo.getUnreplicatedTotal() +
                  keyEntityInfo.getSize());
          openKeyInsightInfo.setReplicatedTotal(
              openKeyInsightInfo.getReplicatedTotal() +
                  keyEntityInfo.getReplicatedSize());
          boolean added =
              isLegacyBucketLayout ? nonFSOKeyInfoList.add(keyEntityInfo) :
                  fsoKeyInfoList.add(keyEntityInfo);
          if ((nonFSOKeyInfoList.size() + fsoKeyInfoList.size()) == limit) {
            recordsFetchedLimitReached = true;
            break;
          }
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
      if (recordsFetchedLimitReached) {
        break;
      }
    }
    return Response.ok(openKeyInsightInfo).build();
  }

  /** This method retrieves set of keys/files/dirs pending for deletion. */
  @GET
  @Path("pendingfordeletionkeyinfo")
  public Response getDeletedKeyInfo(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
      String prevKeyPrefix) {
    KeyInsightInfoResp deletedKeyAndDirInsightInfo = new KeyInsightInfoResp();
    KeyInsightInfoResp pendingForDeletionKeyInfo =
        getPendingForDeletionKeyInfo(limit, prevKeyPrefix,
            deletedKeyAndDirInsightInfo);
    return Response.ok(getPendingForDeletionDirInfo(limit, prevKeyPrefix,
        pendingForDeletionKeyInfo)).build();
  }

  private KeyInsightInfoResp getPendingForDeletionDirInfo(
      int limit, String prevKeyPrefix,
      KeyInsightInfoResp pendingForDeletionKeyInfo) {

    List<KeyEntityInfo> deletedDirInfoList =
        pendingForDeletionKeyInfo.getDeletedDirInfoList();

    Table<String, OmKeyInfo> deletedDirTable =
        omMetadataManager.getDeletedDirTable();
    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            keyIter = deletedDirTable.iterator()) {
      boolean skipPrevKey = false;
      String seekKey = prevKeyPrefix;
      if (StringUtils.isNotBlank(prevKeyPrefix)) {
        skipPrevKey = true;
        Table.KeyValue<String, OmKeyInfo> seekKeyValue =
            keyIter.seek(seekKey);
        // check if RocksDB was able to seek correctly to the given key prefix
        // if not, then return empty result
        // In case of an empty prevKeyPrefix, all the keys are returned
        if (seekKeyValue == null ||
            (StringUtils.isNotBlank(prevKeyPrefix) &&
                !seekKeyValue.getKey().equals(prevKeyPrefix))) {
          return pendingForDeletionKeyInfo;
        }
      }
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        String key = kv.getKey();
        OmKeyInfo omKeyInfo = kv.getValue();
        // skip the prev key if prev key is present
        if (skipPrevKey && key.equals(prevKeyPrefix)) {
          continue;
        }
        KeyEntityInfo keyEntityInfo = new KeyEntityInfo();
        keyEntityInfo.setKey(key);
        keyEntityInfo.setPath(omKeyInfo.getKeyName());
        keyEntityInfo.setInStateSince(omKeyInfo.getCreationTime());
        keyEntityInfo.setSize(omKeyInfo.getDataSize());
        keyEntityInfo.setReplicatedSize(omKeyInfo.getReplicatedSize());
        keyEntityInfo.setReplicationConfig(omKeyInfo.getReplicationConfig());
        pendingForDeletionKeyInfo.setUnreplicatedTotal(
            pendingForDeletionKeyInfo.getUnreplicatedTotal() +
                keyEntityInfo.getSize());
        pendingForDeletionKeyInfo.setReplicatedTotal(
            pendingForDeletionKeyInfo.getReplicatedTotal() +
                keyEntityInfo.getReplicatedSize());
        deletedDirInfoList.add(keyEntityInfo);
        if (deletedDirInfoList.size() == limit) {
          break;
        }
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
    return pendingForDeletionKeyInfo;
  }

  private KeyInsightInfoResp getPendingForDeletionKeyInfo(
      int limit,
      String prevKeyPrefix,
      KeyInsightInfoResp deletedKeyAndDirInsightInfo) {
    List<RepeatedOmKeyInfo> repeatedOmKeyInfoList =
        deletedKeyAndDirInsightInfo.getRepeatedOmKeyInfoList();
    Table<String, RepeatedOmKeyInfo> deletedTable =
        omMetadataManager.getDeletedTable();
    try (
        TableIterator<String, ? extends Table.KeyValue<String,
            RepeatedOmKeyInfo>>
            keyIter = deletedTable.iterator()) {
      boolean skipPrevKey = false;
      String seekKey = prevKeyPrefix;
      if (StringUtils.isNotBlank(prevKeyPrefix)) {
        skipPrevKey = true;
        Table.KeyValue<String, RepeatedOmKeyInfo> seekKeyValue =
            keyIter.seek(seekKey);
        // check if RocksDB was able to seek correctly to the given key prefix
        // if not, then return empty result
        // In case of an empty prevKeyPrefix, all the keys are returned
        if (seekKeyValue == null ||
            (StringUtils.isNotBlank(prevKeyPrefix) &&
                !seekKeyValue.getKey().equals(prevKeyPrefix))) {
          return deletedKeyAndDirInsightInfo;
        }
      }
      while (keyIter.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> kv = keyIter.next();
        String key = kv.getKey();
        RepeatedOmKeyInfo repeatedOmKeyInfo = kv.getValue();
        // skip the prev key if prev key is present
        if (skipPrevKey && key.equals(prevKeyPrefix)) {
          continue;
        }
        updateReplicatedAndUnReplicatedTotal(deletedKeyAndDirInsightInfo,
            repeatedOmKeyInfo);
        repeatedOmKeyInfoList.add(repeatedOmKeyInfo);
        if ((repeatedOmKeyInfoList.size()) == limit) {
          break;
        }
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
    return deletedKeyAndDirInsightInfo;
  }

  private void updateReplicatedAndUnReplicatedTotal(
      KeyInsightInfoResp deletedKeyAndDirInsightInfo,
      RepeatedOmKeyInfo repeatedOmKeyInfo) {
    repeatedOmKeyInfo.getOmKeyInfoList().forEach(omKeyInfo -> {
      deletedKeyAndDirInsightInfo.setUnreplicatedTotal(
          deletedKeyAndDirInsightInfo.getUnreplicatedTotal() +
              omKeyInfo.getDataSize());
      deletedKeyAndDirInsightInfo.setReplicatedTotal(
          deletedKeyAndDirInsightInfo.getReplicatedTotal() +
              omKeyInfo.getReplicatedSize());
    });
  }

  /** This method retrieves set of keys/files/dirs which are mapped to
   * containers in DELETED state in SCM. */
  @GET
  @Path("deletedcontainerkeys")
  public Response getDeletedContainerKeysInfo(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
      String prevKeyPrefix) {
    List<KeysResponse> keysResponseList = new ArrayList<>();
    try {
      Map<Long, ContainerMetadata> omContainers =
          reconContainerMetadataManager.getContainers(-1, 0);
      List<ContainerInfo> deletedStateSCMContainers =
          containerManager.getContainers(HddsProtos.LifeCycleState.DELETED);
      List<Long> deletedStateSCMContainerIds =
          deletedStateSCMContainers.stream()
              .map(containerInfo -> containerInfo.getContainerID()).collect(
                  Collectors.toList());

      List<Long> omContainerIdsMappedToDeletedSCMContainers =
          omContainers.entrySet().stream()
              .filter(
                  map -> deletedStateSCMContainerIds.contains(map.getKey()))
              .map(map -> map.getKey()).collect(Collectors.toList());

      omContainerIdsMappedToDeletedSCMContainers.forEach(containerId -> {
        Response keysForContainer =
            containerEndpoint.getKeysForContainer(containerId, limit,
                prevKeyPrefix);
        KeysResponse keysResponse = (KeysResponse) keysForContainer.getEntity();
        keysResponseList.add(keysResponse);
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
    return Response.ok(keysResponseList).build();
  }
}

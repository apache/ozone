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
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyEntityInfo;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
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
import java.util.Arrays;
import java.util.List;

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
@Path("/keys")
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
  @Path("/open")
  public Response getOpenKeyInfo(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
      String prevKey) {
    KeyInsightInfoResponse openKeyInsightInfo = new KeyInsightInfoResponse();
    List<KeyEntityInfo> nonFSOKeyInfoList =
        openKeyInsightInfo.getNonFSOKeyInfoList();
    boolean skipPrevKeyDone = false;
    boolean isLegacyBucketLayout = true;
    boolean recordsFetchedLimitReached = false;
    String lastKey = "";
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
        String seekKey = prevKey;
        if (!skipPrevKeyDone && StringUtils.isNotBlank(prevKey)) {
          skipPrevKey = true;
          Table.KeyValue<String, OmKeyInfo> seekKeyValue =
              keyIter.seek(seekKey);
          // check if RocksDB was able to seek correctly to the given key prefix
          // if not, then return empty result
          // In case of an empty prevKeyPrefix, all the keys are returned
          if (seekKeyValue == null ||
              (StringUtils.isNotBlank(prevKey) &&
                  !seekKeyValue.getKey().equals(prevKey))) {
            continue;
          }
        }
        while (keyIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
          String key = kv.getKey();
          lastKey = key;
          OmKeyInfo omKeyInfo = kv.getValue();
          // skip the prev key if prev key is present
          if (skipPrevKey && key.equals(prevKey)) {
            skipPrevKeyDone = true;
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
    openKeyInsightInfo.setLastKey(lastKey);
    return Response.ok(openKeyInsightInfo).build();
  }

  private void getPendingForDeletionKeyInfo(
      int limit,
      String prevKey,
      KeyInsightInfoResponse deletedKeyAndDirInsightInfo) {
    List<RepeatedOmKeyInfo> repeatedOmKeyInfoList =
        deletedKeyAndDirInsightInfo.getRepeatedOmKeyInfoList();
    Table<String, RepeatedOmKeyInfo> deletedTable =
        omMetadataManager.getDeletedTable();
    try (
        TableIterator<String, ? extends Table.KeyValue<String,
            RepeatedOmKeyInfo>>
            keyIter = deletedTable.iterator()) {
      boolean skipPrevKey = false;
      String seekKey = prevKey;
      String lastKey = "";
      if (StringUtils.isNotBlank(prevKey)) {
        skipPrevKey = true;
        Table.KeyValue<String, RepeatedOmKeyInfo> seekKeyValue =
            keyIter.seek(seekKey);
        // check if RocksDB was able to seek correctly to the given key prefix
        // if not, then return empty result
        // In case of an empty prevKeyPrefix, all the keys are returned
        if (seekKeyValue == null ||
            (StringUtils.isNotBlank(prevKey) &&
                !seekKeyValue.getKey().equals(prevKey))) {
          return;
        }
      }
      while (keyIter.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> kv = keyIter.next();
        String key = kv.getKey();
        lastKey = key;
        RepeatedOmKeyInfo repeatedOmKeyInfo = kv.getValue();
        // skip the prev key if prev key is present
        if (skipPrevKey && key.equals(prevKey)) {
          continue;
        }
        updateReplicatedAndUnReplicatedTotal(deletedKeyAndDirInsightInfo,
            repeatedOmKeyInfo);
        repeatedOmKeyInfoList.add(repeatedOmKeyInfo);
        if ((repeatedOmKeyInfoList.size()) == limit) {
          break;
        }
      }
      deletedKeyAndDirInsightInfo.setLastKey(lastKey);
    } catch (IOException ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** This method retrieves set of keys/files pending for deletion.
   *
   * limit - limits the number of key/files returned.
   * prevKey - E.g. /vol1/bucket1/key1, this will skip keys till it
   * seeks correctly to the given prevKey.
   * Sample API Response:
   * {
   *   "lastKey": "vol1/bucket1/key1",
   *   "replicatedTotal": -1530804718628866300,
   *   "unreplicatedTotal": -1530804718628866300,
   *   "deletedkeyinfo": [
   *     {
   *       "omKeyInfoList": [
   *         {
   *           "metadata": {},
   *           "objectID": 0,
   *           "updateID": 0,
   *           "parentObjectID": 0,
   *           "volumeName": "sampleVol",
   *           "bucketName": "bucketOne",
   *           "keyName": "key_one",
   *           "dataSize": -1530804718628866300,
   *           "keyLocationVersions": [],
   *           "creationTime": 0,
   *           "modificationTime": 0,
   *           "replicationConfig": {
   *             "replicationFactor": "ONE",
   *             "requiredNodes": 1,
   *             "replicationType": "STANDALONE"
   *           },
   *           "fileChecksum": null,
   *           "fileName": "key_one",
   *           "acls": [],
   *           "path": "0/key_one",
   *           "file": false,
   *           "latestVersionLocations": null,
   *           "replicatedSize": -1530804718628866300,
   *           "fileEncryptionInfo": null,
   *           "objectInfo": "OMKeyInfo{volume='sampleVol', bucket='bucketOne',
   *           key='key_one', dataSize='-1530804718628866186', creationTime='0',
   *           objectID='0', parentID='0', replication='STANDALONE/ONE',
   *           fileChecksum='null}",
   *           "updateIDset": false
   *         }
   *       ]
   *     }
   *   ],
   *   "status": "OK"
   * }
   */
  @GET
  @Path("/deletePending")
  public Response getDeletedKeyInfo(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
      String prevKey) {
    KeyInsightInfoResponse
        deletedKeyInsightInfo = new KeyInsightInfoResponse();
    getPendingForDeletionKeyInfo(limit, prevKey,
        deletedKeyInsightInfo);
    return Response.ok(deletedKeyInsightInfo).build();
  }

  private void getPendingForDeletionDirInfo(
      int limit, String prevKey,
      KeyInsightInfoResponse pendingForDeletionKeyInfo) {

    List<KeyEntityInfo> deletedDirInfoList =
        pendingForDeletionKeyInfo.getDeletedDirInfoList();

    Table<String, OmKeyInfo> deletedDirTable =
        omMetadataManager.getDeletedDirTable();
    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            keyIter = deletedDirTable.iterator()) {
      boolean skipPrevKey = false;
      String seekKey = prevKey;
      String lastKey = "";
      if (StringUtils.isNotBlank(prevKey)) {
        skipPrevKey = true;
        Table.KeyValue<String, OmKeyInfo> seekKeyValue =
            keyIter.seek(seekKey);
        // check if RocksDB was able to seek correctly to the given key prefix
        // if not, then return empty result
        // In case of an empty prevKeyPrefix, all the keys are returned
        if (seekKeyValue == null ||
            (StringUtils.isNotBlank(prevKey) &&
                !seekKeyValue.getKey().equals(prevKey))) {
          return;
        }
      }
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        String key = kv.getKey();
        lastKey = key;
        OmKeyInfo omKeyInfo = kv.getValue();
        // skip the prev key if prev key is present
        if (skipPrevKey && key.equals(prevKey)) {
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
      pendingForDeletionKeyInfo.setLastKey(lastKey);
    } catch (IOException ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** This method retrieves set of directories pending for deletion.
   *
   * limit - limits the number of directories returned.
   * prevKey - E.g. /vol1/bucket1/bucket1/dir1, this will skip dirs till it
   * seeks correctly to the given prevKey.
   * Sample API Response:
   * {
   *   "lastKey": "vol1/bucket1/bucket1/dir1"
   *   "replicatedTotal": -1530804718628866300,
   *   "unreplicatedTotal": -1530804718628866300,
   *   "deletedkeyinfo": [
   *     {
   *       "omKeyInfoList": [
   *         {
   *           "metadata": {},
   *           "objectID": 0,
   *           "updateID": 0,
   *           "parentObjectID": 0,
   *           "volumeName": "sampleVol",
   *           "bucketName": "bucketOne",
   *           "keyName": "key_one",
   *           "dataSize": -1530804718628866300,
   *           "keyLocationVersions": [],
   *           "creationTime": 0,
   *           "modificationTime": 0,
   *           "replicationConfig": {
   *             "replicationFactor": "ONE",
   *             "requiredNodes": 1,
   *             "replicationType": "STANDALONE"
   *           },
   *           "fileChecksum": null,
   *           "fileName": "key_one",
   *           "acls": [],
   *           "path": "0/key_one",
   *           "file": false,
   *           "latestVersionLocations": null,
   *           "replicatedSize": -1530804718628866300,
   *           "fileEncryptionInfo": null,
   *           "objectInfo": "OMKeyInfo{volume='sampleVol', bucket='bucketOne',
   *           key='key_one', dataSize='-1530804718628866186', creationTime='0',
   *           objectID='0', parentID='0', replication='STANDALONE/ONE',
   *           fileChecksum='null}",
   *           "updateIDset": false
   *         }
   *       ]
   *     }
   *   ],
   *   "status": "OK"
   * }
   */
  @GET
  @Path("/deletePending/dirs")
  public Response getDeletedDirInfo(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
      String prevKey) {
    KeyInsightInfoResponse
        deletedDirInsightInfo = new KeyInsightInfoResponse();
    getPendingForDeletionDirInfo(limit, prevKey,
        deletedDirInsightInfo);
    return Response.ok(deletedDirInsightInfo).build();
  }

  private void updateReplicatedAndUnReplicatedTotal(
      KeyInsightInfoResponse deletedKeyAndDirInsightInfo,
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
}

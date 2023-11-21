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

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.recon.api.types.ContainerBlocksInfoWrapper;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.DELETED_BLOCKS;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.PREV_DELETED_BLOCKS_TRANSACTION_ID_DEFAULT_VALUE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;

/**
 * Endpoint to get following information about blocks metadata.
 * Number of blocks pending deletion.
 *     - Blocks pending deletion for open/closing containers.
 *     - Blocks pending deletion for closed containers.
 */
@Path("/blocks")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class BlocksEndPoint {
  private final DBStore scmDBStore;
  private final ReconContainerManager containerManager;

  @Inject
  public BlocksEndPoint(ReconStorageContainerManagerFacade reconSCM) {
    this.containerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    this.scmDBStore = reconSCM.getScmDBStore();
  }

  /**
   * This API returns list of blocks grouped by container state
   * (OPEN/CLOSING/CLOSED).
   * {
   *   "OPEN": [
   *     {
   *       "containerId": 100,
   *       "localIDList": [
   *         1,
   *         2,
   *         3,
   *         4
   *       ],
   *       "localIDCount": 4,
   *       "txID": 1
   *     }
   *   ]
   * }
   * @param limit limits the number of records having list of blocks
   *              grouped by container state (OPEN/CLOSING/CLOSED)
   * @param prevKey deletedBlocks table key to skip records before prevKey
   * @return list of blocks grouped by container state (OPEN/CLOSING/CLOSED)
   */
  @GET
  @Path("/deletePending")
  public Response getBlocksPendingDeletion(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(PREV_DELETED_BLOCKS_TRANSACTION_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_PREVKEY) long prevKey) {
    if (limit < 0 || prevKey < 0) {
      // Send back an empty response
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }
    Map<String, List<ContainerBlocksInfoWrapper>>
        containerStateBlockInfoListMap = new HashMap<>();
    try (
        Table<Long,
            StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction>
            deletedBlocksTXTable = DELETED_BLOCKS.getTable(this.scmDBStore);
        TableIterator<Long, ? extends Table.KeyValue<Long,
            StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction>>
            deletedBlocksTableIterator = deletedBlocksTXTable.iterator()) {
      boolean skipPrevKey = false;
      Long seekKey = prevKey;
      if (prevKey > 0) {
        skipPrevKey = true;
        Table.KeyValue<Long,
            StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction>
            seekKeyValue =
            deletedBlocksTableIterator.seek(seekKey);
        // check if RocksDB was able to seek correctly to the given key prefix
        // if not, then return empty result
        if (seekKeyValue == null) {
          return Response.ok(containerStateBlockInfoListMap).build();
        }
      }
      while (deletedBlocksTableIterator.hasNext()) {
        Table.KeyValue<Long,
            StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction>
            kv = deletedBlocksTableIterator.next();
        Long key = kv.getKey();
        StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
            deletedBlocksTransaction =
            kv.getValue();
        // skip the prev key if prev key is present
        if (skipPrevKey && key.equals(prevKey)) {
          continue;
        }
        long containerID = deletedBlocksTransaction.getContainerID();
        String containerState =
            containerManager.getContainer(ContainerID.valueOf(containerID))
                .getState().name();
        ContainerBlocksInfoWrapper containerBlocksInfoWrapper =
            new ContainerBlocksInfoWrapper();
        containerBlocksInfoWrapper.setContainerID(containerID);
        containerBlocksInfoWrapper.setLocalIDList(
            deletedBlocksTransaction.getLocalIDList());
        containerBlocksInfoWrapper.setLocalIDCount(
            deletedBlocksTransaction.getLocalIDCount());
        containerBlocksInfoWrapper.setTxID(deletedBlocksTransaction.getTxID());
        List<ContainerBlocksInfoWrapper> containerBlocksInfoWrappers;
        if (containerStateBlockInfoListMap.containsKey(containerState)) {
          containerBlocksInfoWrappers =
              containerStateBlockInfoListMap.get(containerState);
        } else {
          containerBlocksInfoWrappers = new ArrayList<>();
          containerStateBlockInfoListMap.put(containerState,
              containerBlocksInfoWrappers);
        }
        containerBlocksInfoWrappers.add(containerBlocksInfoWrapper);
        // limit is applied based on number of containers per state
        if (containerBlocksInfoWrappers.size() >= limit) {
          break;
        }
      }
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.ok(containerStateBlockInfoListMap).build();
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.ratis.execution;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * entry for request execution.
 */
public class OMGateway {
  private static final Logger LOG = LoggerFactory.getLogger(OMGateway.class);
  private final LeaderRequestExecutor leaderExecutor;
  private final FollowerRequestExecutor followerExecutor;
  private final OzoneManager om;
  private final AtomicLong requestInProgress = new AtomicLong(0);
  /**
   * uniqueIndex is used to generate index used in objectId creation uniquely accross OM nodes.
   * This makes use of termIndex for init shifted within 54 bits.
   */
  private AtomicLong uniqueIndex = new AtomicLong();

  public OMGateway(OzoneManager om) throws IOException {
    this.om = om;
    this.leaderExecutor = new LeaderRequestExecutor(om, uniqueIndex);
    this.followerExecutor = new FollowerRequestExecutor(om, uniqueIndex);
    if (om.isLeaderExecutorEnabled() && om.isRatisEnabled()) {
      OzoneManagerRatisServer ratisServer = om.getOmRatisServer();
      ratisServer.getOmBasicStateMachine().registerLeaderNotifier(this::leaderChangeNotifier);
      resetUniqueIndex();
    } else {
      // for non-ratis flow, init with last index
      uniqueIndex.set(om.getLastTrxnIndexForNonRatis());
    }
  }
  public void stop() {
    leaderExecutor.stop();
    followerExecutor.stop();
  }
  public OMResponse submit(OMRequest omRequest) throws ServiceException {
    if (!om.isLeaderReady()) {
      String peerId = om.isRatisEnabled() ? om.getOmRatisServer().getRaftPeerId().toString() : om.getOMNodeId();
      OMLeaderNotReadyException leaderNotReadyException = new OMLeaderNotReadyException(peerId
          + " is not ready to process request yet.");
      throw new ServiceException(leaderNotReadyException);
    }
    executorEnable();
    RequestContext requestContext = new RequestContext();
    requestContext.setRequest(omRequest);
    requestInProgress.incrementAndGet();
    requestContext.setFuture(new CompletableFuture<>());
    CompletableFuture<OMResponse> f = requestContext.getFuture()
        .whenComplete((r, th) -> handleAfterExecution(requestContext, th));
    try {
      // TODO gateway locking: take lock with OMLockDetails
      // TODO scheduling of request to pool
      om.checkLeaderStatus();
      validate(omRequest);
      OMClientRequest omClientRequest = OzoneManagerRatisUtils.createClientRequest(omRequest, om);
      requestContext.setClientRequest(omClientRequest);

      // submit request
      ExecutorType executorType = executorSelector(omRequest);
      if (executorType == ExecutorType.LEADER_COMPATIBLE) {
        leaderExecutor.submit(0, requestContext);
      } else if (executorType == ExecutorType.FOLLOWER) {
        followerExecutor.submit(0, requestContext);
      } else {
        leaderExecutor.submit(0, requestContext);
      }
    } catch (InterruptedException e) {
      requestContext.getFuture().completeExceptionally(e);
      Thread.currentThread().interrupt();
    } catch (Throwable e) {
      requestContext.getFuture().completeExceptionally(e);
    }
    try {
      return f.get();
    } catch (ExecutionException ex) {
      throw new ServiceException(ex.getMessage(), ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ServiceException(ex.getMessage(), ex);
    }
  }

  private void validate(OMRequest omRequest) throws IOException {
    OzoneManagerRequestHandler.requestParamValidation(omRequest);
    // validate prepare state
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    OzoneManagerPrepareState prepareState = om.getPrepareState();
    if (cmdType == OzoneManagerProtocolProtos.Type.Prepare) {
      // Must authenticate prepare requests here, since we must determine
      // whether or not to apply the prepare gate before proceeding with the
      // prepare request.
      UserGroupInformation userGroupInformation =
          UserGroupInformation.createRemoteUser(omRequest.getUserInfo().getUserName());
      if (om.getAclsEnabled() && !om.isAdmin(userGroupInformation)) {
        String message = "Access denied for user " + userGroupInformation + ". "
            + "Superuser privilege is required to prepare ozone managers.";
        throw new OMException(message, OMException.ResultCodes.ACCESS_DENIED);
      } else {
        prepareState.enablePrepareGate();
      }
    }

    // In prepare mode, only prepare and cancel requests are allowed to go
    // through.
    if (!prepareState.requestAllowed(cmdType)) {
      String message = "Cannot apply write request " +
          omRequest.getCmdType().name() + " when OM is in prepare mode.";
      throw new OMException(message, OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED);
    }
  }
  private void handleAfterExecution(RequestContext ctx, Throwable th) {
    // TODO: gateway locking: release lock and OMLockDetails update
    requestInProgress.decrementAndGet();
  }

  public void leaderChangeNotifier(String newLeaderId) {
    boolean isLeader = om.getOMNodeId().equals(newLeaderId);
    if (isLeader) {
      cleanupCache();
      resetUniqueIndex();
    } else {
      leaderExecutor.disableProcessing();
    }
  }

  private void resetUniqueIndex() {
    // Using term index, init counter with setting this as MSB in range of 54 bit. MSB is set so that unique index
    // can be generated by just incrementing counter in LSB.
    // since 2 bit reserved for epoch as MSB, and 8 bit for directory object id generation as LSB
    // so middle 54 bit is the counter that should be unique
    // it can not use ratis index from TransactionInfo table as object Id generation can be at higher pace
    // as due to merge of multiple request ratis update
    TermIndex termIdx = om.getOmRatisServer().getOmBasicStateMachine().getLastAppliedTermIndex();
    long term = termIdx.getTerm();
    int count = 0;
    while (term > 0) {
      count += 1;
      term >>= 1;
    }
    term = termIdx.getTerm();
    term <<= (54 - count);
    uniqueIndex.set(term);
  }

  private void rebuildBucketVolumeCache() throws IOException {
    LOG.info("Rebuild of bucket and volume cache");
    Table<String, OmBucketInfo> bucketTable = om.getMetadataManager().getBucketTable();
    Set<String> cachedBucketKeySet = new HashSet<>();
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> cacheItr = bucketTable.cacheIterator();
    while (cacheItr.hasNext()) {
      cachedBucketKeySet.add(cacheItr.next().getKey().getCacheKey());
    }
    try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>> bucItr = bucketTable.iterator()) {
      while (bucItr.hasNext()) {
        Table.KeyValue<String, OmBucketInfo> next = bucItr.next();
        bucketTable.addCacheEntry(next.getKey(), next.getValue(), -1);
        cachedBucketKeySet.remove(next.getKey());
      }
    }

    // removing extra cache entry
    for (String key : cachedBucketKeySet) {
      bucketTable.addCacheEntry(key, -1);
    }

    Set<String> cachedVolumeKeySet = new HashSet<>();
    Table<String, OmVolumeArgs> volumeTable = om.getMetadataManager().getVolumeTable();
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmVolumeArgs>>> volCacheItr = volumeTable.cacheIterator();
    while (volCacheItr.hasNext()) {
      cachedVolumeKeySet.add(volCacheItr.next().getKey().getCacheKey());
    }
    try (TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>> volItr = volumeTable.iterator()) {
      while (volItr.hasNext()) {
        Table.KeyValue<String, OmVolumeArgs> next = volItr.next();
        volumeTable.addCacheEntry(next.getKey(), next.getValue(), -1);
        cachedVolumeKeySet.remove(next.getKey());
      }
    }

    // removing extra cache entry
    for (String key : cachedVolumeKeySet) {
      volumeTable.addCacheEntry(key, -1);
    }
  }

  public void cleanupCache() {
    // TODO no-cache case, no need re-build bucket/volume cache and cleanup of cache
    LOG.debug("clean all table cache and update bucket/volume with db");
    for (String tbl : om.getMetadataManager().listTableNames()) {
      Table table = om.getMetadataManager().getTable(tbl);
      if (table instanceof TypedTable) {
        ArrayList<Long> epochs = new ArrayList<>(((TypedTable<?, ?>) table).getCache().getEpochEntries().keySet());
        if (!epochs.isEmpty()) {
          table.cleanupCache(epochs);
        }
      }
    }
    try {
      rebuildBucketVolumeCache();
    } catch (IOException e) {
      // retry once, else om down
      try {
        rebuildBucketVolumeCache();
      } catch (IOException ex) {
        String errorMessage = "OM unable to access rocksdb, terminating OM. Error " + ex.getMessage();
        ExitUtils.terminate(1, errorMessage, ex, LOG);
      }
    }
  }
  public void executorEnable() throws ServiceException {
    if (leaderExecutor.isProcessing()) {
      return;
    }
    if (requestInProgress.get() == 0) {
      cleanupCache();
      leaderExecutor.enableProcessing();
    } else {
      LOG.warn("Executor is not enabled, previous request {} is still not cleaned", requestInProgress.get());
      String msg = "Request processing is disabled due to error";
      throw new ServiceException(msg, new OMException(msg, OMException.ResultCodes.INTERNAL_ERROR));
    }
  }

  private ExecutorType executorSelector(OMRequest req) {
    switch (req.getCmdType()) {
    case EchoRPC:
      return ExecutorType.LEADER_OPTIMIZED;
    /* cases with Secret manager cache */
    case GetS3Secret:
    case SetS3Secret:
    case RevokeS3Secret:
    case TenantAssignUserAccessId:
    case TenantRevokeUserAccessId:
    case TenantAssignAdmin:
    case TenantRevokeAdmin:
    /* cases for upgrade */
    case FinalizeUpgrade:
    case Prepare:
    case CancelPrepare:
    /* cases for snapshot db update */
    case PurgeKeys:
    case PurgeDirectories:
    case RenameKey:
    case RenameKeys:
    /* cases for snapshot */
    case SnapshotMoveDeletedKeys:
    case SnapshotPurge:
    case SetSnapshotProperty:
    case CreateSnapshot:
    case DeleteSnapshot:
    case RenameSnapshot:
      return ExecutorType.FOLLOWER;
    default:
      return ExecutorType.LEADER_COMPATIBLE;
    }
  }

  enum ExecutorType {
    LEADER_COMPATIBLE,
    FOLLOWER,
    LEADER_OPTIMIZED
  }
}

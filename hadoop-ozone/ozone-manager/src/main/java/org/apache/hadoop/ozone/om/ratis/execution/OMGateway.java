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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * entry for request execution.
 */
public class OMGateway {
  private static final Logger LOG = LoggerFactory.getLogger(OMGateway.class);
  private final LeaderRequestExecutor leaderExecutor;
  private final OzoneManagerRatisServer ratisServer;
  private final AtomicLong requestInProgress = new AtomicLong(0);

  public OMGateway(OzoneManagerRatisServer ratisServer) {
    this.leaderExecutor = new LeaderRequestExecutor(ratisServer);
    this.ratisServer = ratisServer;
    ratisServer.getOmBasicStateMachine().registerLeaderNotifier(this::leaderChangeNotifier);
  }
  public OMResponse submit(OMRequest omRequest) throws ServiceException {
    if (!ratisServer.getOzoneManager().isLeaderReady()) {
      OMLeaderNotReadyException leaderNotReadyException = new OMLeaderNotReadyException(
          ratisServer.getRaftPeerId().toString() + " is not ready to process request yet.");
      throw new ServiceException(leaderNotReadyException);
    }
    executorEnable();
    RequestContext requestContext = new RequestContext();
    requestContext.setRequest(omRequest);
    requestInProgress.incrementAndGet();
    requestContext.setFuture(new CompletableFuture<>().thenApply(resp -> handleAfterExecution(requestContext)));
    try {
      // TODO gateway locking: take lock with OMLockDetails
      // TODO scheduling of request to pool
      ratisServer.getOzoneManager().checkLeaderStatus();
      leaderExecutor.submit(0, requestContext);
    } catch (InterruptedException e) {
      requestContext.getFuture().completeExceptionally(e);
      Thread.currentThread().interrupt();
    } catch (Throwable e) {
      requestContext.getFuture().completeExceptionally(e);
    }
    try {
      return requestContext.getFuture().get();
    } catch (ExecutionException ex) {
      throw new ServiceException(ex.getMessage(), ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ServiceException(ex.getMessage(), ex);
    }
  }

  private OMResponse handleAfterExecution(RequestContext ctx) {
    // TODO: gateway locking: release lock and OMLockDetails update
    requestInProgress.decrementAndGet();
    return ctx.getResponse();
  }
  
  public void leaderChangeNotifier(String newLeaderId) {
    boolean isLeader = ratisServer.getOzoneManager().getOMNodeId().equals(newLeaderId);
    if (isLeader) {
      cleanupCache(Long.MAX_VALUE);
    } else {
      leaderExecutor.disableProcessing();
    }
  }

  private void rebuildBucketVolumeCache() throws IOException {
    Table<String, OmBucketInfo> bucketTable = ratisServer.getOzoneManager().getMetadataManager().getBucketTable();
    Set<String> cachedBucketKeySet = new HashSet<>();
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> cacheItr = bucketTable.cacheIterator();
    while (cacheItr.hasNext()) {
      cachedBucketKeySet.add(cacheItr.next().getKey().getCacheKey());
    }
    TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>> bucItr = bucketTable.iterator();
    while (bucItr.hasNext()) {
      Table.KeyValue<String, OmBucketInfo> next = bucItr.next();
      bucketTable.addCacheEntry(next.getKey(), next.getValue(), -1);
      cachedBucketKeySet.remove(next.getKey());
    }

    // removing extra cache entry
    for (String key : cachedBucketKeySet) {
      bucketTable.addCacheEntry(key, -1);
    }

    Set<String> cachedVolumeKeySet = new HashSet<>();
    Table<String, OmVolumeArgs> volumeTable = ratisServer.getOzoneManager().getMetadataManager().getVolumeTable();
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmVolumeArgs>>> volCacheItr = volumeTable.cacheIterator();
    while (volCacheItr.hasNext()) {
      cachedVolumeKeySet.add(volCacheItr.next().getKey().getCacheKey());
    }
    TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>> volItr = volumeTable.iterator();
    while (volItr.hasNext()) {
      Table.KeyValue<String, OmVolumeArgs> next = volItr.next();
      volumeTable.addCacheEntry(next.getKey(), next.getValue(), -1);
      cachedVolumeKeySet.remove(next.getKey());
    }

    // removing extra cache entry
    for (String key : cachedVolumeKeySet) {
      volumeTable.addCacheEntry(key, -1);
    }
  }

  public void cleanupCache(long lastIndex) {
    // TODO no-cache case, no need re-build bucket/volume cache and cleanup of cache
    LOG.debug("clean all table cache and update bucket/volume with db");
    for (String tbl : ratisServer.getOzoneManager().getMetadataManager().listTableNames()) {
      ratisServer.getOzoneManager().getMetadataManager().getTable(tbl).cleanupCache(
          Collections.singletonList(lastIndex));
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
      cleanupCache(Long.MAX_VALUE);
      leaderExecutor.enableProcessing();
    } else {
      LOG.warn("Executor is not enabled, previous request {} is still not cleaned", requestInProgress.get());
      String msg = "Request processing is disabled due to error";
      throw new ServiceException(msg, new OMException(msg, OMException.ResultCodes.INTERNAL_ERROR));
    }
  }
}

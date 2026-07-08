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

package org.apache.hadoop.ozone.om.eventlistener;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A narrow set of functionality we are ok with exposing to plugin
 * implementations.
 */
public final class OMEventListenerPluginContextImpl implements OMEventListenerPluginContext {
  private static final Logger LOG = LoggerFactory.getLogger(OMEventListenerPluginContextImpl.class);

  private final OzoneManager ozoneManager;
  private final NotificationCheckpointStrategy checkpointStrategy;

  public OMEventListenerPluginContextImpl(OzoneManager ozoneManager,
                                          NotificationCheckpointStrategy checkpointStrategy) {
    this.ozoneManager = ozoneManager;
    this.checkpointStrategy = checkpointStrategy;
  }

  @Override
  public boolean isLeaderReady() {
    return ozoneManager.isLeaderReady();
  }

  // TODO: should we allow plugins to pass in maxResults or just limit
  // them to some predefined value for safety?  e.g. 10K
  @Override
  public List<OmCompletedRequestInfo> listCompletedRequestInfo(Long startKey, int maxResults) throws IOException {
    return ozoneManager.getMetadataManager().listCompletedRequestInfo(startKey, maxResults);
  }

  @Override
  public NotificationCheckpointStrategy getNotificationCheckpointStrategy() {
    return checkpointStrategy;
  }

  @Override
  public void pruneCompletedRequestInfo(long softLimit, long hardLimit) throws IOException {
    Table<Long, OmCompletedRequestInfo> table = ozoneManager.getMetadataManager().getCompletedRequestInfoTable();

    try {
      // 1. Dynamically retrieve the minimum checkpoint across all active listeners from the strategy
      long minCheckpoint = Long.MAX_VALUE;
      boolean hasCheckpoint = false;

      if (checkpointStrategy != null) {
        Long minVal = checkpointStrategy.getMinimumCheckpoint();
        if (minVal != null) {
          minCheckpoint = minVal;
          hasCheckpoint = true;
        }
      }

      // 2. Determine the soft pruning boundary
      long softPruneBeforeKey = hasCheckpoint ? minCheckpoint - softLimit : -1;

      // 3. Find the latest transaction log index to calculate the hard pruning boundary
      long latestKey = -1;
      try (TableIterator<Long, ? extends Table.KeyValue<Long, OmCompletedRequestInfo>>
              iterator = table.iterator()) {
        iterator.seekToLast();
        if (iterator.hasNext()) {
          latestKey = iterator.next().getKey();
        }
      }

      long hardPruneBeforeKey = latestKey - hardLimit;

      // 4. Calculate the final pruning boundary
      long beforeKey = Math.max(softPruneBeforeKey, hardPruneBeforeKey);
      if (beforeKey > 0) {
        table.deleteRange(0L, beforeKey);
        LOG.info("Pruned records from completedRequestInfoTable older than trxLogIndex {} " +
            "(minCheckpoint={}, softLimit={}, latestKey={}, hardLimit={})",
            beforeKey, hasCheckpoint ? minCheckpoint : "N/A", softLimit, latestKey, hardLimit);
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to prune completedRequestInfoTable range", e);
    }
  }

  // TODO: it feels like this doesn't belong here
  @Override
  public String getThreadNamePrefix() {
    return ozoneManager.getThreadNamePrefix();
  }
}

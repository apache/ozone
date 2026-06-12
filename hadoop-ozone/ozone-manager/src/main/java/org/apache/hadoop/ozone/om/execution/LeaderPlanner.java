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

package org.apache.hadoop.ozone.om.execution;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.PlannedRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReplicatedStateTransition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the leader-side planning of a {@link PlannedRequest}.
 * Drives the execution template and produces a {@link ReplicatedStateTransition}
 * that can be replicated via Ratis and applied as a raw DB patch on all nodes.
 */
public final class LeaderPlanner {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderPlanner.class);

  private final OzoneManager ozoneManager;
  private final ManagedIndexService indexService;

  public LeaderPlanner(OzoneManager ozoneManager, ManagedIndexService indexService) {
    this.ozoneManager = ozoneManager;
    this.indexService = indexService;
  }

  public ReplicatedStateTransition plan(PlannedRequest request) {
    long managedIndex = indexService.getAndIncrement();
    TransitionBuilder builder = new TransitionBuilder(managedIndex);

    try {
      request.preProcess(ozoneManager);
      request.authorize(ozoneManager);

      try {
        request.acquireLocks(ozoneManager);
        request.plan(ozoneManager, builder);
      } finally {
        request.releaseLocks(ozoneManager);
      }

    } catch (Exception ex) {
      LOG.warn("Planning failed for {} (index={}): {}",
          request.getCmdType(), managedIndex, ex.getMessage());
      OMResponse errorResponse = request.buildErrorResponse(ex);
      builder = new TransitionBuilder();
      builder.setResponse(errorResponse);
    }

    return builder.build(managedIndex, request.getCmdType());
  }
}

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

package org.apache.hadoop.ozone.om.multiraft;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL_DEFAULT;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.ozone.om.OzoneManager;

/**
 * Background service to reconcile bucket raft groups state.
 */
public class BucketRaftGroupsReconciler extends BackgroundService {

  private final OzoneManager ozoneManager;

  public BucketRaftGroupsReconciler(OzoneManager ozoneManager) {
    super("OMBucketRaftGroupsReconciler", ozoneManager.getConfiguration()
        .getTimeDuration(OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL,
            OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS,
        1, 0, "OMBucketRaftGroupsReconciler-");
    this.ozoneManager = ozoneManager;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue tasksQueue = new BackgroundTaskQueue();
    tasksQueue.add(new BucketRaftGroupReconciliationTask(ozoneManager));
    return tasksQueue;
  }
}

package org.apache.hadoop.ozone.om.multiraft;

import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.ozone.om.OzoneManager;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BUCKET_RAFT_GROUPS_RECONCILER_INTERVAL_DEFAULT;

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

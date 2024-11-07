package org.apache.hadoop.ozone.om.ratis.execution;

import java.io.IOException;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * control class for leader execution compatibility.
 */
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class LeaderExecutionEnabler {
  private static boolean isLeaderExecutionEnabled = false;
  public static void init(OzoneManager om) {
    isLeaderExecutionEnabled = om.getConfiguration().getBoolean("ozone.om.leader.execution.enabled", false);
  }
  public static boolean optimizedFlow(OzoneManagerProtocolProtos.OMRequest omRequest, OzoneManager om)
      throws IOException {
    if (!isLeaderExecutionEnabled) {
      return false;
    }
    OzoneManagerProtocolProtos.KeyArgs keyArgs;
    String volumeName = "";
    String bucketName = "";
    switch (omRequest.getCmdType()) {
    case CreateKey:
      keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CommitKey:
      keyArgs = omRequest.getCommitKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    default:
      return false;
    }
    OmBucketInfo bucketInfo = OzoneManagerUtils.getResolvedBucketInfo(om.getMetadataManager(), volumeName, bucketName);
    BucketLayout bucketLayout = bucketInfo.getBucketLayout();
    if (bucketLayout == BucketLayout.OBJECT_STORE) {
      return true;
    }
    return false;
  }

  public static boolean isOptimizedApplyTransaction(OzoneManagerProtocolProtos.OMRequest omRequest, OzoneManager om) {
    if (omRequest.getCmdType() == OzoneManagerProtocolProtos.Type.PersistDb) {
      return true;
    }
    return false;
  }
}

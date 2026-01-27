package org.apache.hadoop.ozone.om.response.group;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.io.IOException;

/**
 * Response for BucketRaftGroupsStateUpdate request.
 */
@CleanupTableInfo(cleanupTables = {OmMetadataManagerImpl.MULTI_RAFT_INFO_TABLE})
public class OMBucketRaftGroupsStateUpdateResponse extends OMClientResponse {

  public OMBucketRaftGroupsStateUpdateResponse(OzoneManagerProtocolProtos.OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation) throws IOException {

  }

}

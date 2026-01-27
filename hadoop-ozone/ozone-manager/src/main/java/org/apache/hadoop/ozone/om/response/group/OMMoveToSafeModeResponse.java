package org.apache.hadoop.ozone.om.response.group;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.io.IOException;

/**
 * Response for MoveToSafeMode request.
 */
@CleanupTableInfo
public class OMMoveToSafeModeResponse extends OMClientResponse {

  public OMMoveToSafeModeResponse(OzoneManagerProtocolProtos.OMResponse omResponse) {
    super(omResponse);
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation) throws IOException {

  }

}

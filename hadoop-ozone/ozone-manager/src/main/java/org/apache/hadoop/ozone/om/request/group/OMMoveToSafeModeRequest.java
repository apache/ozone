package org.apache.hadoop.ozone.om.request.group;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.group.OMMoveToSafeModeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.io.IOException;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.MoveOmToSafeMode;

/**
 * Handles move to safe mode request.
 */
public class OMMoveToSafeModeRequest extends OMClientRequest {


  public OMMoveToSafeModeRequest(OzoneManagerProtocolProtos.OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    ozoneManager.getSafeModeManager().moveToSaveMode();

    return new OMMoveToSafeModeResponse(OzoneManagerProtocolProtos.OMResponse.newBuilder()
        .setStatus(OK)
        .setCmdType(MoveOmToSafeMode).build()) {
      @Override
      protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation)
          throws IOException {

      }
    };
  }

}

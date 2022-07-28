package org.apache.hadoop.ozone.om.request;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles EchoRPCRequest.
 */
public class OMEchoRPCRequest extends OMClientRequest {
  public OMEchoRPCRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
         long transactionLogIndex,
         OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    return null;
  }

}

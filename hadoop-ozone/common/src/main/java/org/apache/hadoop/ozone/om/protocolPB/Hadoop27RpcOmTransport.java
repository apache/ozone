package org.apache.hadoop.ozone.om.protocolPB;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

public class Hadoop27RpcOmTransport implements OmTransport {

  @Override
  public OMResponse submitRequest(OMRequest payload) {
    return null;
  }

}

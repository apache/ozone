package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

public interface OmTransport {

  OMResponse submitRequest(OMRequest payload) throws IOException;

}

package org.apache.hadoop.ozone.om.request.validation;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

public interface ResponseFeatureValidator<REQTYPE, RESPTYPE> {

  OzoneManagerProtocolProtos.OMResponse validate(
      OzoneManagerProtocolProtos.OMRequest request, OzoneManagerProtocolProtos.OMResponse response) throws Exception;
}

package org.apache.hadoop.ozone.om.exceptions;

import java.io.IOException;

/**
 * Exception thrown by
 * {@link org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB} when
 * OM leader is not ready to serve requests. This error is thrown when Raft
 * Server returns RatisLeaderNotReadyException.
 */
public class RatisLeaderNotReadyException extends IOException  {

  public RatisLeaderNotReadyException(String message) {
    super(message);
  }
}

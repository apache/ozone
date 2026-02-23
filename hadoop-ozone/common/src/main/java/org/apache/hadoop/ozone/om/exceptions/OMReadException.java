/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.exceptions;

import java.io.IOException;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadException;

/**
 * Exceptions thrown by
 * {@link org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB} when
 * Ratis read fails due reasons such as timeout.
 */
public class OMReadException extends IOException {

  public OMReadException(RaftPeerId currentPeerId, String cause) {
    super("OM: " + currentPeerId  + " read failed. " +
        (cause != null ? "Cause: " + cause : ""));
  }

  /**
   * Convert {@link ReadException} to {@link OMReadException}.
   * @param readException Ratis Read exception.
   * @param currentPeer Current peer.
   * @return OMReadException.
   */
  public static OMReadException convertToOMReadException(
      ReadException readException, RaftPeerId currentPeer) {
    Throwable cause = readException.getCause();
    return new OMReadException(currentPeer,
        cause != null ? cause.getMessage() : null);
  }
}

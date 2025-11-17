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

package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Transport responsible to send messages to OM.
 */
public interface OmTransport {

  /**
   * The main method to send out the request on the defined transport.
   */
  OMResponse submitRequest(OMRequest payload) throws IOException;

  /**
   * Return the addresses of the Ozone Managers, used for delegation token.
   */
  Text getDelegationTokenService();

  /**
   * Optional hook to close all the opened resources.
   */
  void close() throws IOException;
}

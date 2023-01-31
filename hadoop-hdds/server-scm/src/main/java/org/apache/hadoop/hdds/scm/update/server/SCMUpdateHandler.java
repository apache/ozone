/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.update.server;

import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.Type;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UpdateRequest;

/**
 * Interface used by server side to handle client update and server publish.
 */
public interface SCMUpdateHandler {

  /**
   * handle client update request.
   * @param request
   * @param clientInfo
   */
  void handleClientRequest(UpdateRequest request,
      SCMUpdateClientInfo clientInfo);

  /**
   * Handle server broadcast to all clients as needed.
   */
  void onUpdate();

  /**
   * Handle server remove client due to error streaming to the client.
   * @param clientInfo
   */
  void onRemoveClient(SCMUpdateClientInfo clientInfo);

  /**
   * Return the type of Update the handler can handle.
   * @return the type of Update the handler can handle.
   */
  Type getType();
}

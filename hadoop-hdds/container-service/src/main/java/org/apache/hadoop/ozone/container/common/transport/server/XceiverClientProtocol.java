/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.transport.server;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;

/**
 * Security protocol for a secure OzoneManager.
 */
@KerberosInfo(
    serverPrincipal = DFS_DATANODE_KERBEROS_PRINCIPAL_KEY
)
public interface XceiverClientProtocol {

  /**
   * Send the client request, and receive datanode response.
   *
   * @param request the request sent by client
   * @return Token<ContainerCommandResponseProto>
   * @throws IOException
   */
    ContainerCommandResponseProto send(ContainerCommandRequestProto request) throws IOException;
}

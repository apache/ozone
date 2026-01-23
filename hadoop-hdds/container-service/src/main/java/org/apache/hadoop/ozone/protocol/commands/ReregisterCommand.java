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

package org.apache.hadoop.ozone.protocol.commands;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReregisterCommandProto;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;

/**
 * Informs a datanode to register itself with SCM again.
 */
public class ReregisterCommand extends
    SCMCommand<ReregisterCommandProto> {

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.reregisterCommand;
  }

  /**
   * Not implemented for ReregisterCommand.
   *
   * @return cmdId.
   */
  @Override
  public long getId() {
    return 0;
  }

  @Override
  public ReregisterCommandProto getProto() {
    return ReregisterCommandProto
        .newBuilder()
        .build();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType())
        .append(": cmdID: ").append(getId())
        .append(", encodedToken: \"").append(getEncodedToken()).append('"')
        .append(", term: ").append(getTerm())
        .append(", deadlineMsSinceEpoch: ").append(getDeadline());
    return sb.toString();
  }
}

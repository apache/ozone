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

import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DatanodeVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.FinalizeNewDatanodeVersionCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;

/**
 * Asks DataNode to finalize new upgrade version.
 */
public class FinalizeVersionCommand
    extends SCMCommand<FinalizeNewDatanodeVersionCommandProto> {

  private boolean finalizeUpgrade = false;
  private DatanodeVersionProto versionInfo;

  public FinalizeVersionCommand(boolean finalizeNewDatanodeVersion,
                                DatanodeVersionProto versionInfo,
                                long id) {
    super(id);
    finalizeUpgrade = finalizeNewDatanodeVersion;
    this.versionInfo = versionInfo;
  }

  public FinalizeVersionCommand(boolean finalizeNewDatanodeVersion,
                                DatanodeVersionProto versionInfo) {
    super();
    finalizeUpgrade = finalizeNewDatanodeVersion;
    this.versionInfo = versionInfo;
  }

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.finalizeNewDatanodeVersionCommand;
  }

  @Override
  public FinalizeNewDatanodeVersionCommandProto getProto() {
    return FinalizeNewDatanodeVersionCommandProto.newBuilder()
        .setFinalizeNewDatanodeVersion(finalizeUpgrade)
        .setCmdId(getId())
        .setDatanodeVersion(versionInfo)
        .build();
  }

  public static FinalizeVersionCommand getFromProtobuf(
      FinalizeNewDatanodeVersionCommandProto finalizeProto) {
    Objects.requireNonNull(finalizeProto, "finalizeProto == null");
    return new FinalizeVersionCommand(
        finalizeProto.getFinalizeNewDatanodeVersion(),
        finalizeProto.getDatanodeVersion(), finalizeProto.getCmdId());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType())
        .append(": cmdID: ").append(getId())
        .append(", encodedToken: \"").append(getEncodedToken()).append('"')
        .append(", term: ").append(getTerm())
        .append(", deadlineMsSinceEpoch: ").append(getDeadline())
        .append(", finalizeUpgrade: ").append(finalizeUpgrade)
        .append(", versionInfo: ").append(versionInfo);
    return sb.toString();
  }
}

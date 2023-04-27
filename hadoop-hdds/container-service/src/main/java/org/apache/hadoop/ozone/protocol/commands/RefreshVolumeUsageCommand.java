/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.protocol.commands;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos
    .RefreshVolumeUsageCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;

/**
 * Asks datanode to refresh disk usage info immediately.
 */
public class RefreshVolumeUsageCommand
    extends SCMCommand<RefreshVolumeUsageCommandProto> {

  public RefreshVolumeUsageCommand() {
    super();
  }

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.refreshVolumeUsageInfo;
  }

  @Override
  public RefreshVolumeUsageCommandProto getProto() {
    RefreshVolumeUsageCommandProto.Builder builder =
        RefreshVolumeUsageCommandProto
            .newBuilder().setCmdId(getId());
    return builder.build();
  }

  public static RefreshVolumeUsageCommand getFromProtobuf(
      RefreshVolumeUsageCommandProto refreshVolumeUsageProto) {
    Preconditions.checkNotNull(refreshVolumeUsageProto);
    return new RefreshVolumeUsageCommand();
  }
}

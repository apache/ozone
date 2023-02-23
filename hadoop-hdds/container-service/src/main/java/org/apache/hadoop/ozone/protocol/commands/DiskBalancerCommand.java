/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.protocol.commands;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.storage.DiskBalancerConfiguration;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DiskBalancerCommandProto;

/**
 * Informs a datanode to update DiskBalancer status.
 */
public class DiskBalancerCommand extends SCMCommand<DiskBalancerCommandProto> {

  private final HddsProtos.DiskBalancerOpType opType;
  private final DiskBalancerConfiguration diskBalancerConfiguration;

  public DiskBalancerCommand(final HddsProtos.DiskBalancerOpType opType,
      final DiskBalancerConfiguration diskBalancerConfiguration) {
    this.opType = opType;
    this.diskBalancerConfiguration = diskBalancerConfiguration;
  }

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.diskBalancerCommand;
  }

  @Override
  public DiskBalancerCommandProto getProto() {
    DiskBalancerCommandProto.Builder builder = DiskBalancerCommandProto
        .newBuilder().setOpType(opType);
    // Stop command don't have diskBalancerConf
    if (diskBalancerConfiguration != null) {
      builder.setDiskBalancerConf(
          diskBalancerConfiguration.toProtobufBuilder());
    }
    return builder.build();
  }

  public static DiskBalancerCommand getFromProtobuf(DiskBalancerCommandProto
      diskbalancerCommandProto, ConfigurationSource configuration) {
    Preconditions.checkNotNull(diskbalancerCommandProto);
    return new DiskBalancerCommand(diskbalancerCommandProto.getOpType(),
        DiskBalancerConfiguration.fromProtobuf(
            diskbalancerCommandProto.getDiskBalancerConf(), configuration));
  }

  public HddsProtos.DiskBalancerOpType getOpType() {
    return opType;
  }

  public DiskBalancerConfiguration getDiskBalancerConfiguration() {
    return diskBalancerConfiguration;
  }
}

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

package org.apache.hadoop.hdds.protocol;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.GetDiskBalancerInfoRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.ozone.ClientVersion;

/**
 * Client-to-datanode RPC protocol for administering DiskBalancer.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface DiskBalancerProtocol extends Closeable {

  long VERSIONID = 1L;

  GetDiskBalancerInfoRequestProto DEFAULT_GET_DISK_BALANCER_INFO_REQUEST
      = GetDiskBalancerInfoRequestProto.newBuilder().setClientVersion(ClientVersion.CURRENT.serialize()).build();

  @Idempotent
  default DatanodeDiskBalancerInfoProto getDiskBalancerInfo() throws IOException {
    return getDiskBalancerInfo(DEFAULT_GET_DISK_BALANCER_INFO_REQUEST);
  }

  /**
   * Get DiskBalancer information for a datanode.
   * 
   * @return DiskBalancer information for the datanode
   * @throws IOException on RPC or serialization errors
   */
  @Idempotent
  DatanodeDiskBalancerInfoProto getDiskBalancerInfo(GetDiskBalancerInfoRequestProto request) throws IOException;

  /**
   * Start DiskBalancer on the datanode using the provided configuration. When
   * the configuration is {@code null}, the datanode uses its last persisted
   * configuration.
   *
   * @param config optional configuration overrides
   * @throws IOException if the operation fails
   */
  @Idempotent
  void startDiskBalancer(@Nullable HddsProtos.DiskBalancerConfigurationProto config)
      throws IOException;

  /**
   * Stop DiskBalancer on the datanode.
   *
   * @throws IOException if the operation fails
   */
  @Idempotent
  void stopDiskBalancer() throws IOException;

  /**
   * Update DiskBalancer configuration.
   *
   * @param config new configuration values
   * @throws IOException if the operation fails
   */
  @Idempotent
  void updateDiskBalancerConfiguration(HddsProtos.DiskBalancerConfigurationProto config)
      throws IOException;
}



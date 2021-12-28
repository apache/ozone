/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.recon.codec;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerReplicaHistoryListProto;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaHistoryList;

import java.io.IOException;

/**
 * Codec for ContainerReplicaHistoryList.
 */
public class ContainerReplicaHistoryListCodec
    implements Codec<ContainerReplicaHistoryList> {

  @Override
  public byte[] toPersistedFormat(ContainerReplicaHistoryList obj) {
    return obj.toProto().toByteArray();
  }

  @Override
  public ContainerReplicaHistoryList fromPersistedFormat(byte[] rawData)
      throws IOException {
    return ContainerReplicaHistoryList.fromProto(
        ContainerReplicaHistoryListProto.parseFrom(rawData));
  }

  @Override
  public ContainerReplicaHistoryList copyObject(
      ContainerReplicaHistoryList obj) {
    return obj;
  }
}

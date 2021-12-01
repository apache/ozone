/*
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

package org.apache.hadoop.ozone.recon.scm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerReplicaHistoryListProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerReplicaHistoryProto;

/**
 * A list of ContainerReplicaHistory.
 *
 * For Recon DB table definition.
 */
public class ContainerReplicaHistoryList {

  private List<ContainerReplicaHistory> replicaHistories;

  public ContainerReplicaHistoryList(
      List<ContainerReplicaHistory> replicaHistories) {
    this.replicaHistories = new ArrayList<>(replicaHistories);
  }

  public List<ContainerReplicaHistory> asList() {
    return Collections.unmodifiableList(replicaHistories);
  }

  public List<ContainerReplicaHistory> getList() {
    return replicaHistories;
  }

  public static ContainerReplicaHistoryList fromProto(
      ContainerReplicaHistoryListProto proto) {
    List<ContainerReplicaHistory> replicaHistoryList = new ArrayList<>();
    for (ContainerReplicaHistoryProto rhProto : proto.getReplicaHistoryList()) {
      replicaHistoryList.add(ContainerReplicaHistory.fromProto(rhProto));
    }
    return new ContainerReplicaHistoryList(replicaHistoryList);
  }

  public ContainerReplicaHistoryListProto toProto() {
    ContainerReplicaHistoryListProto.Builder builder =
        ContainerReplicaHistoryListProto.newBuilder();
    replicaHistories.stream()
        .map(ContainerReplicaHistory::toProto)
        .forEach(builder::addReplicaHistory);
    return builder.build();
  }

}

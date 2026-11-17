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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CreatePipelineCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

/**
 * Asks datanode to create a pipeline.
 */
public class CreatePipelineCommand
    extends SCMCommand<CreatePipelineCommandProto> {

  private static final Integer HIGH_PRIORITY = 1;
  private static final Integer LOW_PRIORITY = 0;
  private static final List<Integer> DEFAULT_PRIORITY_LIST
      = Collections.nCopies(ReplicationFactor.THREE_VALUE, LOW_PRIORITY);

  private final PipelineID pipelineID;
  private final ReplicationFactor factor;
  private final ReplicationType type;
  private final List<DatanodeDetails> nodelist;
  private final List<Integer> priorityList;

  public CreatePipelineCommand(final PipelineID pipelineID,
      final ReplicationType type, final ReplicationFactor factor,
      final List<DatanodeDetails> datanodeList,
      final DatanodeDetails suggestedLeader) {
    super();
    this.pipelineID = pipelineID;
    this.factor = factor;
    this.type = type;
    this.nodelist = datanodeList;
    this.priorityList = newPriorityList(datanodeList, suggestedLeader);
  }

  static List<Integer> newPriorityList(List<DatanodeDetails> dns, DatanodeDetails suggestedLeader) {
    if (suggestedLeader == null) {
      if (dns.size() == DEFAULT_PRIORITY_LIST.size()) {
        return DEFAULT_PRIORITY_LIST;
      } else {
        return Collections.nCopies(dns.size(), LOW_PRIORITY);
      }
    }

    final List<Integer> priorities = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      if (dn.equals(suggestedLeader)) {
        priorities.add(HIGH_PRIORITY);
      } else {
        priorities.add(LOW_PRIORITY);
      }
    }
    return Collections.unmodifiableList(priorities);
  }

  public CreatePipelineCommand(long cmdId, final PipelineID pipelineID,
      final ReplicationType type, final ReplicationFactor factor,
      final List<DatanodeDetails> datanodeList,
      final List<Integer> priorityList) {
    super(cmdId);
    this.pipelineID = pipelineID;
    this.factor = factor;
    this.type = type;
    this.nodelist = datanodeList;
    this.priorityList = priorityList;
  }

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.createPipelineCommand;
  }

  @Override
  public CreatePipelineCommandProto getProto() {
    return CreatePipelineCommandProto.newBuilder()
        .setCmdId(getId())
        .setPipelineID(pipelineID.getProtobuf())
        .setFactor(factor)
        .setType(type)
        .addAllDatanode(nodelist.stream()
            .map(DatanodeDetails::getProtoBufMessage)
            .collect(Collectors.toList()))
        .addAllPriority(priorityList)
        .build();
  }

  public static CreatePipelineCommand getFromProtobuf(
      CreatePipelineCommandProto createPipelineProto) {
    Objects.requireNonNull(createPipelineProto, "createPipelineProto == null");
    return new CreatePipelineCommand(createPipelineProto.getCmdId(),
        PipelineID.getFromProtobuf(createPipelineProto.getPipelineID()),
        createPipelineProto.getType(), createPipelineProto.getFactor(),
        createPipelineProto.getDatanodeList().stream()
            .map(DatanodeDetails::getFromProtoBuf)
            .collect(Collectors.toList()),
        createPipelineProto.getPriorityList());
  }

  public PipelineID getPipelineID() {
    return pipelineID;
  }

  public List<DatanodeDetails> getNodeList() {
    return nodelist;
  }

  public List<Integer> getPriorityList() {
    return priorityList;
  }

  public ReplicationType getReplicationType() {
    return type;
  }

  public ReplicationFactor getFactor() {
    return factor;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType())
        .append(": cmdID: ").append(getId())
        .append(", encodedToken: \"").append(getEncodedToken()).append('"')
        .append(", term: ").append(getTerm())
        .append(", deadlineMsSinceEpoch: ").append(getDeadline())
        .append(", pipelineID: ").append(getPipelineID())
        .append(", replicationFactor: ").append(factor)
        .append(", replicationType: ").append(type)
        .append(", nodelist: ").append(nodelist)
        .append(", priorityList: ").append(priorityList);
    return sb.toString();
  }
}

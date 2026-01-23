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

package org.apache.hadoop.hdds.scm.container.common.helpers;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

/**
 * This class contains set of dns and containers which ozone client provides
 * to be handed over to SCM when block allocation request comes.
 */
public class ExcludeList {

  private final Map<DatanodeDetails, Long> datanodes = new ConcurrentHashMap<>();
  private final Set<ContainerID> containerIds = new HashSet<>();
  private final Set<PipelineID> pipelineIds = new HashSet<>();
  private long expiryTime = 0;
  private final Clock clock;

  public ExcludeList() {
    clock = Clock.system(ZoneOffset.UTC);
  }

  public ExcludeList(long autoExpiryTime, Clock clock) {
    this.expiryTime = autoExpiryTime;
    this.clock = clock;
  }

  public Set<ContainerID> getContainerIds() {
    return Collections.unmodifiableSet(containerIds);
  }

  public Set<DatanodeDetails> getDatanodes() {
    Set<DatanodeDetails> dns = new HashSet<>();
    if (expiryTime > 0) {
      Iterator<Map.Entry<DatanodeDetails, Long>> iterator =
          datanodes.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<DatanodeDetails, Long> entry = iterator.next();
        Long storedExpiryTime = entry.getValue();
        if (clock.millis() > storedExpiryTime) {
          iterator.remove(); // removing
        } else {
          dns.add(entry.getKey());
        }
      }
    } else {
      dns = datanodes.keySet();
    }
    return dns;
  }

  public void addDatanodes(Collection<DatanodeDetails> dns) {
    dns.forEach(dn -> addDatanode(dn));
  }

  public void addDatanode(DatanodeDetails dn) {
    datanodes.put(dn, clock.millis() + expiryTime);
  }

  public void addConatinerId(ContainerID containerId) {
    containerIds.add(containerId);
  }

  public void addPipeline(PipelineID pipelineId) {
    pipelineIds.add(pipelineId);
  }

  public Set<PipelineID> getPipelineIds() {
    return Collections.unmodifiableSet(pipelineIds);
  }

  public HddsProtos.ExcludeListProto getProtoBuf() {
    HddsProtos.ExcludeListProto.Builder builder =
        HddsProtos.ExcludeListProto.newBuilder();
    containerIds
        .forEach(id -> builder.addContainerIds(id.getId()));
    getDatanodes().forEach(dn -> builder.addDatanodes(dn.getUuidString()));
    pipelineIds.forEach(pipelineID -> {
      builder.addPipelineIds(pipelineID.getProtobuf());
    });
    return builder.build();
  }

  public static ExcludeList getFromProtoBuf(
      HddsProtos.ExcludeListProto excludeListProto) {
    ExcludeList excludeList = new ExcludeList();
    excludeListProto.getContainerIdsList().forEach(id -> {
      excludeList.addConatinerId(ContainerID.valueOf(id));
    });
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    excludeListProto.getDatanodesList().forEach(dn -> {
      builder.setUuid(UUID.fromString(dn));
      excludeList.addDatanode(builder.build());
    });
    excludeListProto.getPipelineIdsList().forEach(pipelineID -> {
      excludeList.addPipeline(PipelineID.getFromProtobuf(pipelineID));
    });
    return excludeList;
  }

  public boolean isEmpty() {
    return getDatanodes().isEmpty() && containerIds.isEmpty() && pipelineIds
        .isEmpty();
  }

  public void clear() {
    datanodes.clear();
    containerIds.clear();
    pipelineIds.clear();
  }

  @Override
  public String toString() {
    return "ExcludeList {" +
        "datanodes = " + datanodes.keySet() +
        ", containerIds = " + containerIds +
        ", pipelineIds = " + pipelineIds +
        '}';
  }

  public long getExpiryTime() {
    return expiryTime;
  }
}

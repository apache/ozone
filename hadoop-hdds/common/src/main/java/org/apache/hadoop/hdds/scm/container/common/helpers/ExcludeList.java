/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.common.helpers;


import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.util.Daemon;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * This class contains set of dns and containers which ozone client provides
 * to be handed over to SCM when block allocation request comes.
 */
public class ExcludeList {

  private final Map<DatanodeDetails, Long> datanodes;
  private final Set<ContainerID> containerIds;
  private final Set<PipelineID> pipelineIds;
  private Daemon excludeNodesCleaner;
  private boolean autoCleanerRunning;


  public ExcludeList() {
    datanodes = new HashMap<>();
    containerIds = new HashSet<>();
    pipelineIds = new HashSet<>();
  }

  public void startAutoExcludeNodesCleaner(long expiryTime,
      long recheckInterval) {
    excludeNodesCleaner =
        new Daemon(new ExcludeNodesCleaner(expiryTime, recheckInterval));
    excludeNodesCleaner.start();
  }

  public void stopAutoExcludeNodesCleaner() {
    if (excludeNodesCleaner != null) {
      autoCleanerRunning = false;
      excludeNodesCleaner.interrupt();
    }
  }

  public Set<ContainerID> getContainerIds() {
    return containerIds;
  }

  public Set<DatanodeDetails> getDatanodes() {
    return datanodes.keySet();
  }

  public void addDatanodes(Collection<DatanodeDetails> dns) {
    dns.forEach(dn -> addDatanode(dn));
  }

  public void addDatanode(DatanodeDetails dn) {
    synchronized (datanodes) {
      datanodes.put(dn, monotonicNow());
    }
  }

  public void addConatinerId(ContainerID containerId) {
    containerIds.add(containerId);
  }

  public void addPipeline(PipelineID pipelineId) {
    pipelineIds.add(pipelineId);
  }

  public Set<PipelineID> getPipelineIds() {
    return pipelineIds;
  }

  public HddsProtos.ExcludeListProto getProtoBuf() {
    HddsProtos.ExcludeListProto.Builder builder =
        HddsProtos.ExcludeListProto.newBuilder();
    containerIds
        .forEach(id -> builder.addContainerIds(id.getId()));
    synchronized (datanodes) {
      datanodes.forEach((dn, t) -> builder.addDatanodes(dn.getUuidString()));
    }
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
    return datanodes.isEmpty() && containerIds.isEmpty() && pipelineIds
        .isEmpty();
  }

  public void clear() {
    stopAutoExcludeNodesCleaner();
    synchronized (datanodes) {
      datanodes.clear();
    }
    containerIds.clear();
    pipelineIds.clear();
  }

  @Override
  public String toString() {
    return "ExcludeList {" +
        "datanodes = " + datanodes +
        ", containerIds = " + containerIds +
        ", pipelineIds = " + pipelineIds +
        '}';
  }

  /*
   * A periodic thread that scans for exclude nodes that are cached for long.
   */
  class ExcludeNodesCleaner implements Runnable {
    private long expiryTime;
    private long recheckInterval;

    ExcludeNodesCleaner(long expiry, long recheckTime) {
      this.expiryTime = expiry;
      this.recheckInterval = recheckTime;
    }

    @Override
    public void run() {
      autoCleanerRunning = true;
      while (autoCleanerRunning) {
        try {
          check();
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
          // Someone interrupted. So, exiting from this thread.
          autoCleanerRunning = false;
        }
      }
    }

    private void check() {

      synchronized (datanodes) {
        Iterator<Map.Entry<DatanodeDetails, Long>> iterator =
            datanodes.entrySet().iterator();
        long now = monotonicNow();
        while (iterator.hasNext()) {
          Map.Entry<DatanodeDetails, Long> entry = iterator.next();
          Long value = entry.getValue();
          if (now > value + expiryTime) {
            iterator.remove(); // removing
          }
        }
      }
    }
  }

}

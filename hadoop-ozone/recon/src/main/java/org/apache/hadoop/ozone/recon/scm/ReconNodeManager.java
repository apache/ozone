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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;

import com.google.common.collect.ImmutableSet;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reregisterCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon SCM's Node manager that includes persistence.
 */
public class ReconNodeManager extends SCMNodeManager {

  public static final Logger LOG = LoggerFactory
      .getLogger(ReconNodeManager.class);

  private Table<UUID, DatanodeDetails> nodeDB;
  private final static Set<Type> ALLOWED_COMMANDS =
      ImmutableSet.of(reregisterCommand);

  /**
   * Map that contains mapping between datanodes
   * and their last heartbeat time.
   */
  private Map<UUID, Long> datanodeHeartbeatMap = new HashMap<>();

  public ReconNodeManager(OzoneConfiguration conf,
                          SCMStorageConfig scmStorageConfig,
                          EventPublisher eventPublisher,
                          NetworkTopology networkTopology,
                          Table<UUID, DatanodeDetails> nodeDB) {
    super(conf, scmStorageConfig, eventPublisher, networkTopology);
    this.nodeDB = nodeDB;
    loadExistingNodes();
  }

  private void loadExistingNodes() {
    try {
      int nodeCount = 0;
      TableIterator<UUID, ? extends Table.KeyValue<UUID, DatanodeDetails>>
          iterator = nodeDB.iterator();
      while (iterator.hasNext()) {
        DatanodeDetails datanodeDetails = iterator.next().getValue();
        register(datanodeDetails, null, null);
        nodeCount++;
      }
      LOG.info("Loaded {} nodes from node DB.", nodeCount);
    } catch (IOException ioEx) {
      LOG.error("Exception while loading existing nodes.", ioEx);
    }
  }

  /**
   * Add a new new node to the NodeDB. Must be called after register.
   * @param datanodeDetails Datanode details.
   */
  public void addNodeToDB(DatanodeDetails datanodeDetails) throws IOException {
    nodeDB.put(datanodeDetails.getUuid(), datanodeDetails);
    LOG.info("Adding new node {} to Node DB.", datanodeDetails.getUuid());
  }

  /**
   * Returns the last heartbeat time of the given node.
   *
   * @param datanodeDetails DatanodeDetails
   * @return last heartbeat time
   */
  public long getLastHeartbeat(DatanodeDetails datanodeDetails) {
    return datanodeHeartbeatMap.getOrDefault(datanodeDetails.getUuid(), 0L);
  }

  @Override
  public void onMessage(CommandForDatanode commandForDatanode,
                        EventPublisher ignored) {
    if (ALLOWED_COMMANDS.contains(
        commandForDatanode.getCommand().getType())) {
      super.onMessage(commandForDatanode, ignored);
    } else {
      LOG.info("Ignoring unsupported command {} for Datanode {}.",
          commandForDatanode.getCommand().getType(),
          commandForDatanode.getDatanodeId());
    }
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param datanodeDetails - DatanodeDetailsProto.
   * @return SCMheartbeat response.
   */
  @Override
  public List<SCMCommand> processHeartbeat(DatanodeDetails datanodeDetails) {
    // Update heartbeat map with current time
    datanodeHeartbeatMap.put(datanodeDetails.getUuid(), Time.now());
    return super.processHeartbeat(datanodeDetails);
  }
}

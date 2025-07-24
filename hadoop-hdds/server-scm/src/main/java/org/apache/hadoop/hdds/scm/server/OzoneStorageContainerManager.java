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

package org.apache.hadoop.hdds.scm.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancer;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;

/**
 * Interface for the SCM Facade class that can be used by a passive SCM like
 * Recon to tweak implementation.
 */
public interface OzoneStorageContainerManager {

  void start() throws IOException;

  void stop();

  void join();

  void shutDown(String message);

  NodeManager getScmNodeManager();

  BlockManager getScmBlockManager();

  PipelineManager getPipelineManager();

  ContainerManager getContainerManager();

  ReplicationManager getReplicationManager();

  ContainerBalancer getContainerBalancer();

  InetSocketAddress getDatanodeRpcAddress();

  SCMNodeDetails getScmNodeDetails();

  ReconfigurationHandler getReconfigurationHandler();

  SCMMetadataStore getScmMetadataStore();

  SCMHAManager getScmHAManager();

  SequenceIdGenerator getSequenceIdGen();
}

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

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of pipeline state manager.
 * PipelineStateMap class holds the data structures related to pipeline and its
 * state. All the read and write operations in PipelineStateMap are protected
 * by a read write lock.
 */
public final class PipelineStateManagerImpl implements PipelineStateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineStateManagerImpl.class);

  private PipelineStateMap pipelineStateMap;
  private final NodeManager nodeManager;
  private Table<PipelineID, Pipeline> pipelineStore;
  private final DBTransactionBuffer transactionBuffer;

  // Protect potential contentions between RaftServer and PipelineManager.
  // See https://issues.apache.org/jira/browse/HDDS-4560
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private PipelineStateManagerImpl(
      Table<PipelineID, Pipeline> pipelineStore, NodeManager nodeManager,
      DBTransactionBuffer buffer) {
    this.pipelineStateMap = new PipelineStateMap();
    this.nodeManager = nodeManager;
    this.pipelineStore = pipelineStore;
    this.transactionBuffer = buffer;
  }

  private void initialize() throws RocksDatabaseException, CodecException, DuplicatedPipelineIdException {
    Objects.requireNonNull(pipelineStore, "pipelineStore == null");
    Objects.requireNonNull(nodeManager, "nodeManager == null");
    if (pipelineStore.isEmpty()) {
      LOG.info("No pipeline exists in current db");
      return;
    }
    try (TableIterator<PipelineID, Pipeline> iterator = pipelineStore.valueIterator()) {
      while (iterator.hasNext()) {
        final Pipeline pipeline = iterator.next();
        pipelineStateMap.addPipeline(pipeline);
        nodeManager.addPipeline(pipeline);
      }
    }
  }

  @Override
  public void addPipeline(HddsProtos.Pipeline pipelineProto)
      throws DuplicatedPipelineIdException, RocksDatabaseException, CodecException {
    Pipeline pipeline = Pipeline.getFromProtobuf(pipelineProto);
    lock.writeLock().lock();
    try {
      if (pipelineStore != null) {
        pipelineStateMap.addPipeline(pipeline);
        nodeManager.addPipeline(pipeline);
        transactionBuffer
            .addToBuffer(pipelineStore, pipeline.getId(), pipeline);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void addContainerToPipeline(PipelineID pipelineId, ContainerID containerID)
      throws PipelineNotFoundException, InvalidPipelineStateException {
    lock.writeLock().lock();
    try {
      pipelineStateMap.addContainerToPipeline(pipelineId, containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void addContainerToPipelineForce(PipelineID pipelineId, ContainerID containerID)
      throws PipelineNotFoundException {
    lock.writeLock().lock();
    try {
      pipelineStateMap.addContainerToPipelineSCMStart(pipelineId, containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Pipeline getPipeline(PipelineID pipelineID)
      throws PipelineNotFoundException {
    lock.readLock().lock();
    try {
      return pipelineStateMap.getPipeline(pipelineID);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines() {
    lock.readLock().lock();
    try {
      return pipelineStateMap.getPipelines();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig) {
    lock.readLock().lock();
    try {
      return pipelineStateMap.getPipelines(replicationConfig);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig,
      Pipeline.PipelineState state) {
    lock.readLock().lock();
    try {
      return pipelineStateMap.getPipelines(replicationConfig, state);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<Pipeline> getPipelines(
      ReplicationConfig replicationConfig,
      Pipeline.PipelineState state, Collection<DatanodeDetails> excludeDns,
      Collection<PipelineID> excludePipelines) {
    lock.readLock().lock();
    try {
      return pipelineStateMap
          .getPipelines(replicationConfig, state, excludeDns, excludePipelines);
    } finally {
      lock.readLock().unlock();
    }
  }


  /**
   * Returns the count of pipelines meeting the given ReplicationConfig and
   * state.
   * @param replicationConfig The ReplicationConfig of the pipelines to count
   * @param state The current state of the pipelines to count
   * @return The count of pipelines meeting the above criteria
   */
  @Override
  public int getPipelineCount(
      ReplicationConfig replicationConfig,
      Pipeline.PipelineState state) {
    lock.readLock().lock();
    try {
      return pipelineStateMap.getPipelineCount(replicationConfig, state);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public NavigableSet<ContainerID> getContainers(PipelineID pipelineID) throws PipelineNotFoundException {
    lock.readLock().lock();
    try {
      return pipelineStateMap.getContainers(pipelineID);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int getNumberOfContainers(PipelineID pipelineID) throws PipelineNotFoundException {
    lock.readLock().lock();
    try {
      return pipelineStateMap.getNumberOfContainers(pipelineID);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void removePipeline(HddsProtos.PipelineID pipelineIDProto)
      throws InvalidPipelineStateException, RocksDatabaseException, CodecException {
    PipelineID pipelineID = PipelineID.getFromProtobuf(pipelineIDProto);
    try {
      Pipeline pipeline;
      lock.writeLock().lock();
      try {
        pipeline = pipelineStateMap.removePipeline(pipelineID);
        nodeManager.removePipeline(pipeline);
        if (pipelineStore != null) {
          transactionBuffer.removeFromBuffer(pipelineStore, pipelineID);
        }
      } finally {
        lock.writeLock().unlock();
      }
    } catch (PipelineNotFoundException pnfe) {
      LOG.warn("Pipeline {} is not found in the pipeline Map. Pipeline"
          + " may have been deleted already.", pipelineIDProto.getId());
    }
  }

  @Override
  public void removeContainerFromPipeline(PipelineID pipelineID, ContainerID containerID) {
    try {
      lock.writeLock().lock();
      try {
        // Typically, SCM can send a pipeline close Action to datanode and
        // receive pipelineCloseAction to close the pipeline which will remove
        // the pipelineId both from the piplineStateMap as well as
        // pipeline2containerMap Subsequently, close container handler event can
        // also try to close the container as a part of which , it will also
        // try to remove the container from the pipeline2container Map which
        // will fail with PipelineNotFoundException. These are executed over
        // ratis, and if the exception is propagated to SCMStateMachine, it will
        // bring down the SCM. Ignoring it here.
        pipelineStateMap.removeContainerFromPipeline(pipelineID, containerID);
      } finally {
        lock.writeLock().unlock();
      }
    } catch (PipelineNotFoundException pnfe) {
      LOG.info("Pipeline {} is not found in the pipeline2ContainerMap. Pipeline"
          + " may have been closed already.", pipelineID);
    }
  }

  @Override
  public void updatePipelineState(HddsProtos.PipelineID pipelineIDProto, HddsProtos.PipelineState newState)
      throws RocksDatabaseException, CodecException {
    PipelineID pipelineID = PipelineID.getFromProtobuf(pipelineIDProto);
    Pipeline.PipelineState newPipelineState =
        Pipeline.PipelineState.fromProtobuf(newState);
    try {
      lock.writeLock().lock();
      try {
        // null check is here to prevent the case where SCM store
        // is closed but the staleNode handlers/pipeline creations
        // still try to access it.
        if (pipelineStore != null) {
          pipelineStateMap.updatePipelineState(pipelineID, newPipelineState);
          transactionBuffer
              .addToBuffer(pipelineStore, pipelineID, getPipeline(pipelineID));
        }
      } finally {
        lock.writeLock().unlock();
      }
    } catch (PipelineNotFoundException pnfe) {
      LOG.warn("Pipeline {} is not found in the pipeline Map. Pipeline"
          + " may have been deleted already.", pipelineID);
    } catch (IOException ex) {
      LOG.error("Pipeline {} state update failed", pipelineID);
      throw ex;
    }
  }

  @Override
  public void close() {
    lock.writeLock().lock();
    try {
      pipelineStore = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void reinitialize(Table<PipelineID, Pipeline> store)
      throws RocksDatabaseException, DuplicatedPipelineIdException, CodecException {
    lock.writeLock().lock();
    try {
      this.pipelineStateMap = new PipelineStateMap();
      this.pipelineStore = store;
      initialize();
    } catch (Exception ex) {
      LOG.error("PipelineManager reinitialization close failed", ex);
      throw ex;
    } finally {
      lock.writeLock().unlock();
    }
  }

  // legacy interfaces end
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for PipelineStateManagerImpl.
   */
  public static class Builder {
    private Table<PipelineID, Pipeline> pipelineStore;
    private NodeManager nodeManager;
    private SCMRatisServer scmRatisServer;
    private DBTransactionBuffer transactionBuffer;

    public Builder setSCMDBTransactionBuffer(DBTransactionBuffer buffer) {
      this.transactionBuffer = buffer;
      return this;
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public Builder setNodeManager(final NodeManager scmNodeManager) {
      nodeManager = scmNodeManager;
      return this;
    }

    public Builder setPipelineStore(
        final Table<PipelineID, Pipeline> pipelineTable) {
      this.pipelineStore = pipelineTable;
      return this;
    }

    public PipelineStateManager build() throws RocksDatabaseException, DuplicatedPipelineIdException, CodecException {
      Objects.requireNonNull(pipelineStore, "pipelineStore == null");

      final PipelineStateManagerImpl pipelineStateManager = new PipelineStateManagerImpl(
          pipelineStore, nodeManager, transactionBuffer);
      pipelineStateManager.initialize();

      return scmRatisServer.getProxyHandler(RequestType.PIPELINE,
          PipelineStateManager.class, pipelineStateManager);
    }
  }
}

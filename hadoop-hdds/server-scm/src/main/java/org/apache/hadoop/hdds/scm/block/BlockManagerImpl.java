/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.block;

import javax.management.ObjectName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.INVALID_BLOCK_SIZE;
import static org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.LOCAL_ID;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;

import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Block Manager manages the block access for SCM. */
public class BlockManagerImpl implements BlockManager, BlockmanagerMXBean {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockManagerImpl.class);
  // TODO : FIX ME : Hard coding the owner.
  // Currently only user of the block service is Ozone, CBlock manages blocks
  // by itself and does not rely on the Block service offered by SCM.

  private final StorageContainerManager scm;
  private final PipelineManager pipelineManager;
  private final ContainerManagerV2 containerManager;

  private final long containerSize;

  private DeletedBlockLog deletedBlockLog;
  private final SCMBlockDeletingService blockDeletingService;

  private ObjectName mxBean;
  private final PipelineChoosePolicy pipelineChoosePolicy;
  private final SequenceIdGenerator sequenceIdGen;

  /**
   * Constructor.
   *
   * @param conf - configuration.
   * @param scm
   * @throws IOException
   */
  public BlockManagerImpl(final ConfigurationSource conf,
                          final StorageContainerManager scm)
      throws IOException {
    Objects.requireNonNull(scm, "SCM cannot be null");
    this.scm = scm;
    this.pipelineManager = scm.getPipelineManager();
    this.containerManager = scm.getContainerManager();
    this.pipelineChoosePolicy = scm.getPipelineChoosePolicy();
    this.sequenceIdGen = scm.getSequenceIdGen();
    this.containerSize = (long)conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);

    mxBean = MBeans.register("BlockManager", "BlockManagerImpl", this);

    // SCM block deleting transaction log and deleting service.
    deletedBlockLog = new DeletedBlockLogImplV2(conf,
        scm.getContainerManager(),
        scm.getScmHAManager().getRatisServer(),
        scm.getScmMetadataStore().getDeletedBlocksTXTable(),
        scm.getScmHAManager().getDBTransactionBuffer(),
        scm.getScmContext(),
        scm.getSequenceIdGen());
    Duration svcInterval = conf.getObject(
            ScmConfig.class).getBlockDeletionInterval();
    long serviceTimeout =
        conf.getTimeDuration(
            OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
            OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);
    blockDeletingService =
        new SCMBlockDeletingService(deletedBlockLog, containerManager,
            scm.getScmNodeManager(), scm.getEventQueue(), scm.getScmContext(),
            scm.getSCMServiceManager(), svcInterval, serviceTimeout, conf);
  }

  /**
   * Start block manager services.
   *
   * @throws IOException
   */
  @Override
  public void start() throws IOException {
    this.blockDeletingService.start();
  }

  /**
   * Shutdown block manager services.
   *
   * @throws IOException
   */
  @Override
  public void stop() throws IOException {
    this.blockDeletingService.shutdown();
    this.close();
  }

  /**
   * Allocates a block in a container and returns that info.
   *
   * @param size - Block Size
   * @param replicationConfig - Replication config
   * @param owner - Owner (service) of the container.
   * @param excludeList List of datanodes/containers to exclude during block
   *                    allocation.
   * @return Allocated block
   * @throws IOException on failure.
   */
  @Override
  public AllocatedBlock allocateBlock(final long size,
      ReplicationConfig replicationConfig,
      String owner, ExcludeList excludeList)
      throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Size : {} , replicationConfig: {}", size, replicationConfig);
    }
    if (scm.getScmContext().isInSafeMode()) {
      throw new SCMException("SafeModePrecheck failed for allocateBlock",
          SCMException.ResultCodes.SAFE_MODE_EXCEPTION);
    }
    if (size < 0 || size > containerSize) {
      LOG.warn("Invalid block size requested : {}", size);
      throw new SCMException("Unsupported block size: " + size,
          INVALID_BLOCK_SIZE);
    }

    /*
      Here is the high level logic.

      1. We try to find pipelines in open state.

      2. If there are no pipelines in OPEN state, then we try to create one.

      3. We allocate a block from the available containers in the selected
      pipeline.

      TODO : #CLUTIL Support random picking of two containers from the list.
      So we can use different kind of policies.
    */

    ContainerInfo containerInfo;

    //TODO we need to continue the refactor to use ReplicationConfig everywhere
    //in downstream managers.

    while (true) {
      List<Pipeline> availablePipelines =
          pipelineManager
              .getPipelines(replicationConfig, Pipeline.PipelineState.OPEN,
                  excludeList.getDatanodes(), excludeList.getPipelineIds());
      Pipeline pipeline = null;
      if (availablePipelines.size() == 0 && !excludeList.isEmpty()) {
        // if no pipelines can be found, try finding pipeline without
        // exclusion
        availablePipelines = pipelineManager
            .getPipelines(replicationConfig, Pipeline.PipelineState.OPEN);
      }
      if (availablePipelines.size() == 0) {
        try {
          // TODO: #CLUTIL Remove creation logic when all replication types and
          // factors are handled by pipeline creator
          pipeline = pipelineManager.createPipeline(replicationConfig);

          // wait until pipeline is ready
          pipelineManager.waitPipelineReady(pipeline.getId(), 0);
        } catch (SCMException se) {
          LOG.warn("Pipeline creation failed for replicationConfig {} " +
              "Datanodes may be used up.", replicationConfig, se);
          break;
        } catch (IOException e) {
          LOG.warn("Pipeline creation failed for replicationConfig: {}. "
              + "Retrying get pipelines call once.", replicationConfig, e);
          availablePipelines = pipelineManager
              .getPipelines(replicationConfig, Pipeline.PipelineState.OPEN,
                  excludeList.getDatanodes(), excludeList.getPipelineIds());
          if (availablePipelines.size() == 0 && !excludeList.isEmpty()) {
            // if no pipelines can be found, try finding pipeline without
            // exclusion
            availablePipelines = pipelineManager
                .getPipelines(replicationConfig, Pipeline.PipelineState.OPEN);
          }
          if (availablePipelines.size() == 0) {
            LOG.info(
                "Could not find available pipeline of replicationConfig: {} "
                    + "even after retrying",
                replicationConfig);
            break;
          }
        }
      }

      if (null == pipeline) {
        PipelineRequestInformation pri =
            PipelineRequestInformation.Builder.getBuilder()
                .setSize(size)
                .build();
        pipeline = pipelineChoosePolicy.choosePipeline(
            availablePipelines, pri);
      }

      // look for OPEN containers that match the criteria.
      containerInfo = containerManager.getMatchingContainer(size, owner,
          pipeline, excludeList.getContainerIds());

      if (containerInfo != null) {
        return newBlock(containerInfo);
      }
    }

    // we have tried all strategies we know and but somehow we are not able
    // to get a container for this block. Log that info and return a null.
    LOG.error(
        "Unable to allocate a block for the size: {}, replicationConfig: {}",
        size, replicationConfig);
    return null;
  }

  /**
   * newBlock - returns a new block assigned to a container.
   *
   * @param containerInfo - Container Info.
   * @return AllocatedBlock
   */
  private AllocatedBlock newBlock(ContainerInfo containerInfo)
      throws NotLeaderException {
    try {
      final Pipeline pipeline = pipelineManager
          .getPipeline(containerInfo.getPipelineID());
      long localID = sequenceIdGen.getNextId(LOCAL_ID);
      long containerID = containerInfo.getContainerID();
      AllocatedBlock.Builder abb =  new AllocatedBlock.Builder()
          .setContainerBlockID(new ContainerBlockID(containerID, localID))
          .setPipeline(pipeline);
      if (LOG.isTraceEnabled()) {
        LOG.trace("New block allocated : {} Container ID: {}", localID,
            containerID);
      }
      pipelineManager.incNumBlocksAllocatedMetric(pipeline.getId());
      return abb.build();
    } catch (PipelineNotFoundException ex) {
      LOG.error("Pipeline Machine count is zero.", ex);
      return null;
    }
  }

  /**
   * Deletes a list of blocks in an atomic operation. Internally, SCM writes
   * these blocks into a
   * {@link DeletedBlockLog} and deletes them from SCM DB. If this is
   * successful, given blocks are
   * entering pending deletion state and becomes invisible from SCM namespace.
   *
   * @param keyBlocksInfoList . This is the list of BlockGroup which contains
   * groupID of keys and list of BlockIDs associated with them.
   * @throws IOException if exception happens, non of the blocks is deleted.
   */
  @Override
  public void deleteBlocks(List<BlockGroup> keyBlocksInfoList)
      throws IOException {
    if (scm.getScmContext().isInSafeMode()) {
      throw new SCMException("SafeModePrecheck failed for deleteBlocks",
          SCMException.ResultCodes.SAFE_MODE_EXCEPTION);
    }
    Map<Long, List<Long>> containerBlocks = new HashMap<>();
    // TODO: track the block size info so that we can reclaim the container
    // TODO: used space when the block is deleted.
    for (BlockGroup bg : keyBlocksInfoList) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting blocks {}",
            StringUtils.join(",", bg.getBlockIDList()));
      }
      for (BlockID block : bg.getBlockIDList()) {
        long containerID = block.getContainerID();
        if (containerBlocks.containsKey(containerID)) {
          containerBlocks.get(containerID).add(block.getLocalID());
        } else {
          List<Long> item = new ArrayList<>();
          item.add(block.getLocalID());
          containerBlocks.put(containerID, item);
        }
      }
    }

    try {
      deletedBlockLog.addTransactions(containerBlocks);
    } catch (IOException e) {
      throw new IOException("Skip writing the deleted blocks info to"
          + " the delLog because addTransaction fails. " + keyBlocksInfoList
          .size() + "Keys skipped", e);
    }
    // TODO: Container report handling of the deleted blocks:
    // Remove tombstone and update open container usage.
    // We will revisit this when the closed container replication is done.
  }

  @Override
  public DeletedBlockLog getDeletedBlockLog() {
    return this.deletedBlockLog;
  }

  /**
   * Close the resources for BlockManager.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (deletedBlockLog != null) {
      deletedBlockLog.close();
    }
    blockDeletingService.shutdown();
    if (mxBean != null) {
      MBeans.unregister(mxBean);
      mxBean = null;
    }
  }

  @Override
  public int getOpenContainersNo() {
    return 0;
    // TODO : FIX ME : The open container being a single number does not make
    // sense.
    // We have to get open containers by Replication Type and Replication
    // factor. Hence returning 0 for now.
    // containers.get(HddsProtos.LifeCycleState.OPEN).size();
  }

  @Override
  public SCMBlockDeletingService getSCMBlockDeletingService() {
    return this.blockDeletingService;
  }

  /**
   * Get class logger.
   * */
  public static Logger getLogger() {
    return LOG;
  }

  /**
   * This class uses system current time milliseconds to generate unique id.
   */
}

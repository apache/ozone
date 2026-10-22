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

package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.XceiverClientSpi.Validator;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.io.grpc.Status;

/**
 * Abstract class used as an interface for input streams related to Ozone
 * blocks.
 */
public abstract class BlockExtendedInputStream extends ExtendedInputStream
    implements PartInputStream {

  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BlockExtendedInputStream.class);

  protected static final List<Validator> VALIDATORS =
      ContainerProtocolCalls.toValidatorList((request, response) -> validate(response));

  public abstract BlockID getBlockID();

  @Override
  public long getRemaining() {
    return getLength() - getPos();
  }

  @Override
  public abstract long getLength();

  @Override
  public abstract long getPos();

  protected Pipeline setPipeline(Pipeline pipeline) throws IOException {
    if (pipeline == null) {
      return null;
    }
    long replicaIndexes = pipeline.getNodes().stream().mapToInt(pipeline::getReplicaIndex).distinct().count();

    if (replicaIndexes > 1) {
      throw new IOException(String.format("Pipeline: %s has nodes containing different replica indexes.",
          pipeline));
    }

    // irrespective of the container state, we will always read via Standalone protocol.
    boolean okForRead = pipeline.getType() == HddsProtos.ReplicationType.STAND_ALONE
            || pipeline.getType() == HddsProtos.ReplicationType.EC;
    return okForRead ? pipeline : pipeline.copyForRead();
  }

  protected boolean shouldRetryRead(IOException cause, RetryPolicy retryPolicy, int retries) throws IOException {
    RetryPolicy.RetryAction retryAction;
    try {
      retryAction = retryPolicy.shouldRetry(cause, retries, 0, true);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
    if (retryAction.action == RetryPolicy.RetryAction.RetryDecision.RETRY) {
      if (retryAction.delayMillis > 0) {
        try {
          LOG.debug("Retry read after {}ms", retryAction.delayMillis);
          Thread.sleep(retryAction.delayMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          String msg = "Interrupted: action=" + retryAction.action + ", retry policy=" + retryPolicy;
          throw new IOException(msg, e);
        }
      }
      return true;
    }
    return false;
  }

  protected BlockData getBlockDataUsingGRPCClient(XceiverClientSpi client, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token) throws IOException {
    LOG.debug("Get block info for block {}", getBlockID());
    return ContainerProtocolCalls.getBlock(client, VALIDATORS, getBlockID(), token, pipeline).getBlockData();
  }

  private static void validate(ContainerCommandResponseProto response) throws IOException {
    if (!response.hasGetBlock()) {
      throw new IllegalArgumentException("Not GetBlock: response=" + response);
    }
    final GetBlockResponseProto b = response.getGetBlock();
    final long blockLength = b.getBlockData().getSize();
    final List<ChunkInfo> chunks = b.getBlockData().getChunksList();
    for (int i = 0; i < chunks.size(); i++) {
      final ChunkInfo c = chunks.get(i);
      // HDDS-10682 caused an empty chunk to get written to the end of some EC blocks. Due to this
      // validation, these blocks will not be readable. In the EC case, the empty chunk is always
      // the last chunk and the offset is the block length. We can safely ignore this case and not fail.
      if (c.getLen() <= 0 && i == chunks.size() - 1 && c.getOffset() == blockLength) {
        DatanodeBlockID blockID = b.getBlockData().getBlockID();
        LOG.warn("The last chunk is empty for container/block {}/{} with an offset of the block length. " +
            "Likely due to HDDS-10682. This is safe to ignore.", blockID.getContainerID(), blockID.getLocalID());
      } else if (c.getLen() <= 0) {
        throw new IOException("Failed to get chunkInfo["
            + i + "]: len == " + c.getLen());
      }
    }
  }

  protected RetryPolicy getReadRetryPolicy(OzoneClientConfig config) {
    return HddsClientUtils.createRetryPolicy(config.getMaxReadRetryCount(),
        TimeUnit.SECONDS.toMillis(config.getReadRetryInterval()));
  }

  protected void refreshBlockInfo(IOException cause, BlockID blockID, AtomicReference<Pipeline> pipelineRef,
      AtomicReference<Token<OzoneBlockTokenIdentifier>> tokenRef, Function<BlockID, BlockLocationInfo> refreshFunction)
      throws IOException {
    LOG.info("Attempting to update pipeline and block token for block {} from pipeline {}: {}",
        blockID, pipelineRef.get().getId(), cause.getMessage());
    if (refreshFunction != null) {
      LOG.debug("Re-fetching pipeline and block token for block {}", blockID);
      BlockLocationInfo blockLocationInfo = refreshFunction.apply(blockID);
      if (blockLocationInfo == null) {
        LOG.warn("No new block location info for block {}", blockID);
      } else {
        pipelineRef.set(setPipeline(blockLocationInfo.getPipeline()));
        LOG.info("New pipeline for block {}: {}", blockID, blockLocationInfo.getPipeline());

        tokenRef.set(blockLocationInfo.getToken());
        if (blockLocationInfo.getToken() != null) {
          OzoneBlockTokenIdentifier tokenId = new OzoneBlockTokenIdentifier();
          tokenId.readFromByteArray(tokenRef.get().getIdentifier());
          LOG.info("A new token is added for block {}. Expiry: {}", blockID,
              Instant.ofEpochMilli(tokenId.getExpiryDate()));
        }
      }
    } else {
      throw cause;
    }
  }

  /**
   * Check if this exception is because datanodes are not reachable.
   */
  protected boolean isConnectivityIssue(IOException ex) {
    return Status.fromThrowable(ex).getCode() == Status.UNAVAILABLE.getCode();
  }

}

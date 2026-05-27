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

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ratis.util.function.CheckedBiConsumer;

/**
 * A Client for the storageContainer protocol.
 */
public abstract class XceiverClientSpi implements Closeable {
  private final AtomicInteger referenceCount;
  private boolean isEvicted;

  /**
   * Validator for container command request/response.
   */
  public interface Validator extends
      CheckedBiConsumer<ContainerCommandRequestProto,
          ContainerCommandResponseProto, IOException> {
    // just a shortcut to avoid having to repeat long list of generic parameters
  }

  public XceiverClientSpi() {
    this.referenceCount = new AtomicInteger(0);
    this.isEvicted = false;
  }

  void incrementReference() {
    this.referenceCount.incrementAndGet();
  }

  void decrementReference() {
    this.referenceCount.decrementAndGet();
    cleanup();
  }

  void setEvicted() {
    isEvicted = true;
    cleanup();
  }

  // close the xceiverClient only if,
  // 1) there is no refcount on the client
  // 2) it has been evicted from the cache.
  private void cleanup() {
    if (referenceCount.get() == 0 && isEvicted) {
      close();
    }
  }

  @VisibleForTesting
  public int getRefcount() {
    return referenceCount.get();
  }

  /**
   * Connects to the leader in the pipeline.
   */
  public abstract void connect() throws Exception;

  @Override
  public abstract void close();

  /**
   * Returns the pipeline of machines that host the container used by this
   * client.
   *
   * @return pipeline of machines that host the container
   */
  public abstract Pipeline getPipeline();

  /**
   * Sends a given command to server and gets the reply back.
   * @param request Request
   * @return Response to the command
   */
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request) throws IOException {
    try {
      XceiverClientReply reply = sendCommandAsync(request);
      return reply.getResponse().get();
    } catch (InterruptedException e) {
      // Re-interrupt the thread while catching InterruptedException
      Thread.currentThread().interrupt();
      throw getIOExceptionForSendCommand(request, e);
    } catch (ExecutionException e) {
      throw getIOExceptionForSendCommand(request, e);
    }
  }

  /**
   * Sends a given command to server and gets the reply back along with
   * the server associated info.
   * @param request Request
   * @param validators functions to validate the response
   * @return Response to the command
   */
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request,
      List<Validator> validators)
      throws IOException {
    ContainerCommandResponseProto responseProto = null;
    boolean validatorsPassed = false;
    try {
      XceiverClientReply reply = sendCommandAsync(request);
      responseProto = reply.getResponse().get();
      for (Validator function : validators) {
        function.accept(request, responseProto);
      }
      validatorsPassed = true;
      return responseProto;
    } catch (InterruptedException e) {
      // Re-interrupt the thread while catching InterruptedException
      Thread.currentThread().interrupt();
      throw getIOExceptionForSendCommand(request, e);
    } catch (ExecutionException e) {
      throw getIOExceptionForSendCommand(request, e);
    } finally {
      // If a validator threw, the caller never receives the response, so
      // release any zero-copy-tracked buffer here. Successful responses are
      // returned to the caller, who is responsible for releasing them.
      if (responseProto != null && !validatorsPassed) {
        releaseReceivedResponse(responseProto);
      }
    }
  }

  /**
   * Sends a given command using an explicit zero-copy-capable response path
   * when supported by the transport.
   * <p>
   * The default implementation delegates to {@link #sendCommand(
   * ContainerCommandRequestProto, List)}, so transports without zero-copy
   * support continue to return normal heap-backed responses.
   *
   * @param request Request
   * @param validators functions to validate the response
   * @return Response to the command
   */
  public ContainerCommandResponseProto sendCommandWithZeroCopy(
      ContainerCommandRequestProto request,
      List<Validator> validators) throws IOException {
    return sendCommand(request, validators);
  }

  public void initStreamRead(BlockID blockID, StreamingReaderSpi streamObserver) throws IOException {
    throw new UnsupportedOperationException("Stream read is not supported");
  }

  public void streamRead(ContainerCommandRequestProto request, StreamingReadResponse streamObserver) {
    throw new UnsupportedOperationException("Stream read is not supported");
  }

  public void completeStreamRead() {
    throw new UnsupportedOperationException("Stream read is not supported");
  }

  public static IOException getIOExceptionForSendCommand(
      ContainerCommandRequestProto request, Exception e) {
    return new IOException("Failed to execute command "
        + HddsUtils.processForDebug(request), e);
  }

  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   */
  public abstract XceiverClientReply
      sendCommandAsync(ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException;

  /**
   * Returns pipeline Type.
   *
   * @return - {Stand_Alone, Ratis or Chained}
   */
  public abstract HddsProtos.ReplicationType getPipelineType();

  /**
   * Check if an specific commitIndex is replicated to majority/all servers.
   * @param index index to watch for
   * @return reply containing the min commit index replicated to all or majority
   *         servers in case of a failure
   */
  public CompletableFuture<XceiverClientReply> watchForCommit(long index) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * returns the min commit index replicated to all servers.
   * @return min commit index replicated to all servers.
   */
  public abstract long getReplicatedMinCommitIndex();

  /**
   * Sends command to all nodes in the pipeline.
   * @return a map containing datanode as the key and
   * the command response from that datanode
   */
  public abstract Map<DatanodeDetails, ContainerCommandResponseProto>
      sendCommandOnAllNodes(ContainerCommandRequestProto request)
      throws IOException, InterruptedException;

  /**
   * Release the resources held on behalf of a previously-received response.
   * <p>
   * When the underlying transport parses a response with a zero-copy
   * marshaller, the parsed proto's {@code bytes} fields reference the
   * Netty-managed pooled buffer of the inbound message. Those buffers must
   * be released back to Netty when the caller is done with the proto;
   * otherwise direct memory accumulates. Callers of
   * {@code sendCommandWithZeroCopy} that retain the response past the call
   * (e.g. {@code ReadChunk} via
   * {@code ContainerProtocolCalls.readChunkForZeroCopy(...)} in
   * {@code ChunkInputStream}) must invoke this method once they are done.
   * <p>
   * This method is idempotent and safe to call on responses that were
   * never tracked by a zero-copy marshaller.
   */
  public void releaseReceivedResponse(ContainerCommandResponseProto response) {
    // Default: transport without zero-copy support -> no-op.
  }
}

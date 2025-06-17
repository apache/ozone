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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.util.Time;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.Preconditions;

/**
 * DispatcherContext class holds transport protocol specific context info
 * required for execution of container commands over the container dispatcher.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class DispatcherContext {
  private static final DispatcherContext HANDLE_READ_CHUNK
      = newBuilder(Op.HANDLE_READ_CHUNK).build();
  private static final DispatcherContext HANDLE_READ_BLOCK
      = newBuilder(Op.HANDLE_READ_BLOCK).build();
  private static final DispatcherContext HANDLE_WRITE_CHUNK
      = newBuilder(Op.HANDLE_WRITE_CHUNK).build();
  private static final DispatcherContext HANDLE_GET_SMALL_FILE
      = newBuilder(Op.HANDLE_GET_SMALL_FILE).build();
  private static final DispatcherContext HANDLE_PUT_SMALL_FILE
      = newBuilder(Op.HANDLE_PUT_SMALL_FILE).build();

  private final Op op;
  // whether the chunk data needs to be written or committed or both
  private final WriteChunkStage stage;
  // which term the request is being served in Ratis
  private final long term;
  // the log index in Ratis log to which the request belongs to
  private final long logIndex;

  private final Map<Long, Long> container2BCSIDMap;

  private final boolean releaseSupported;
  private volatile Runnable releaseMethod;

  private final long startTime = Time.monotonicNowNanos();

  public static DispatcherContext getHandleReadChunk() {
    return HANDLE_READ_CHUNK;
  }

  public static DispatcherContext getHandleReadBlock() {
    return HANDLE_READ_BLOCK;
  }

  public static DispatcherContext getHandleWriteChunk() {
    return HANDLE_WRITE_CHUNK;
  }

  public static DispatcherContext getHandleGetSmallFile() {
    return HANDLE_GET_SMALL_FILE;
  }

  public static DispatcherContext getHandlePutSmallFile() {
    return HANDLE_PUT_SMALL_FILE;
  }

  /**
   * Determines which stage of writeChunk a write chunk request is for.
   */
  public enum WriteChunkStage {
    WRITE_DATA, COMMIT_DATA, COMBINED;

    public boolean isWrite() {
      return this != COMMIT_DATA;
    }

    public boolean isCommit() {
      return this != WRITE_DATA;
    }
  }

  /** Operation types. */
  public enum Op {
    NULL,

    HANDLE_READ_CHUNK,
    HANDLE_READ_BLOCK,
    HANDLE_WRITE_CHUNK,
    HANDLE_GET_SMALL_FILE,
    HANDLE_PUT_SMALL_FILE,

    READ_STATE_MACHINE_DATA,
    WRITE_STATE_MACHINE_DATA,
    APPLY_TRANSACTION,

    STREAM_INIT,
    STREAM_LINK;

    public boolean readFromTmpFile() {
      return this == READ_STATE_MACHINE_DATA;
    }

    public boolean validateToken() {
      switch (this) {
      case APPLY_TRANSACTION:
      case WRITE_STATE_MACHINE_DATA:
      case READ_STATE_MACHINE_DATA:
      case STREAM_LINK:
        return false;
      default:
        return true;
      }
    }
  }

  public static Op op(DispatcherContext context) {
    return context == null ? Op.NULL : context.getOp();
  }

  private DispatcherContext(Builder b) {
    this.op = Objects.requireNonNull(b.op, "op == null");
    this.term = b.term;
    this.logIndex = b.logIndex;
    this.stage = b.stage;
    this.container2BCSIDMap = b.container2BCSIDMap;
    this.releaseSupported = b.releaseSupported;
  }

  /** Use {@link DispatcherContext#op(DispatcherContext)} for handling null. */
  private Op getOp() {
    return op;
  }

  public long getLogIndex() {
    return logIndex;
  }

  public long getTerm() {
    return term;
  }

  public WriteChunkStage getStage() {
    return stage;
  }

  public Map<Long, Long> getContainer2BCSIDMap() {
    return container2BCSIDMap;
  }

  public long getStartTime() {
    return startTime;
  }

  public boolean isReleaseSupported() {
    return releaseSupported;
  }

  public void setReleaseMethod(Runnable releaseMethod) {
    Preconditions.assertTrue(releaseSupported, "Unsupported release method");
    this.releaseMethod = releaseMethod;
  }

  public void release() {
    if (releaseMethod != null) {
      releaseMethod.run();
    }
  }

  @Override
  public String toString() {
    return op + "-" + stage + TermIndex.valueOf(term, logIndex);
  }

  public static Builder newBuilder(Op op) {
    return new Builder(Objects.requireNonNull(op, "op == null"));
  }

  /**
   * Builder class for building DispatcherContext.
   */
  public static final class Builder {
    private final Op op;
    private WriteChunkStage stage = WriteChunkStage.COMBINED;
    private long term;
    private long logIndex;
    private Map<Long, Long> container2BCSIDMap;
    private boolean releaseSupported;

    private Builder(Op op) {
      this.op = op;
    }

    /**
     * Sets the WriteChunkStage.
     *
     * @param writeChunkStage WriteChunk Stage
     * @return DispatcherContext.Builder
     */
    public Builder setStage(WriteChunkStage writeChunkStage) {
      this.stage = writeChunkStage;
      return this;
    }

    /**
     * Sets the current term for the container request from Ratis.
     *
     * @param currentTerm current term
     * @return DispatcherContext.Builder
     */
    public Builder setTerm(long currentTerm) {
      this.term = currentTerm;
      return this;
    }

    /**
     * Sets the logIndex for the container request from Ratis.
     *
     * @param index log index
     * @return DispatcherContext.Builder
     */
    public Builder setLogIndex(long index) {
      this.logIndex = index;
      return this;
    }

    /**
     * Sets the container2BCSIDMap to contain all the containerIds per
     * RaftGroup.
     * @param map container2BCSIDMap
     * @return Builder
     */
    public Builder setContainer2BCSIDMap(Map<Long, Long> map) {
      this.container2BCSIDMap = map;
      return this;
    }

    public Builder setReleaseSupported(boolean releaseSupported) {
      this.releaseSupported = releaseSupported;
      return this;
    }

    /**
     * Builds and returns DispatcherContext instance.
     *
     * @return DispatcherContext
     */
    public DispatcherContext build() {
      return new DispatcherContext(this);
    }

  }
}

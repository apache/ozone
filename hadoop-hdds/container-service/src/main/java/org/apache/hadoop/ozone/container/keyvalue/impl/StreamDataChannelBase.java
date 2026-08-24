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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import static org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil.onFailure;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.util.Time;
import org.apache.ratis.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For write state machine data.
 */
abstract class StreamDataChannelBase
    implements StateMachine.DataChannel {
  static final Logger LOG = LoggerFactory.getLogger(
      StreamDataChannelBase.class);

  private final RandomAccessFile randomAccessFile;

  private final File file;
  private final AtomicBoolean linked = new AtomicBoolean();
  private final AtomicBoolean cleaned = new AtomicBoolean();

  private final ContainerData containerData;
  private final ContainerMetrics metrics;

  StreamDataChannelBase(File file, ContainerData containerData,
                        ContainerMetrics metrics)
      throws StorageContainerException {
    try {
      this.file = file;
      this.randomAccessFile = new RandomAccessFile(file, "rw");
    } catch (FileNotFoundException e) {
      throw new StorageContainerException("BlockFile not exists with " +
          "container Id " + containerData.getContainerID() +
          " file " + file.getAbsolutePath(),
          ContainerProtos.Result.IO_EXCEPTION);
    }
    this.containerData = containerData;
    this.metrics = metrics;
  }

  abstract ContainerProtos.Type getType();

  private FileChannel getChannel() {
    return randomAccessFile.getChannel();
  }

  protected void checkVolume() {
    onFailure(containerData.getVolume());
  }

  @Override
  public final void force(boolean metadata) throws IOException {
    try {
      getChannel().force(metadata);
    } catch (IOException e) {
      checkVolume();
      throw e;
    }
  }

  @Override
  public final boolean isOpen() {
    return getChannel().isOpen();
  }

  protected void assertSpaceAvailability(int requested) throws StorageContainerException {
    ContainerUtils.assertSpaceAvailability(containerData.getContainerID(), containerData.getVolume(), requested);
  }

  public void setLinked() {
    linked.set(true);
  }

  /**
   * @return true if {@link org.apache.ratis.statemachine.StateMachine.DataChannel} is already linked.
   */
  public boolean cleanUp() {
    if (linked.get()) {
      // already linked, nothing to do.
      return true;
    }
    if (cleaned.compareAndSet(false, true)) {
      // close and then delete the file.
      try {
        cleanupInternal();
      } catch (IOException e) {
        LOG.warn("Failed to close " + this, e);
      }
    }
    return false;
  }

  protected abstract void cleanupInternal() throws IOException;

  @Override
  public void close() throws IOException {
    try {
      randomAccessFile.close();
    } catch (IOException e) {
      checkVolume();
      throw e;
    }
  }

  final int writeFileChannel(ByteBuffer src) throws IOException {
    try {
      final long startTime = Time.monotonicNowNanos();
      final int writeBytes = getChannel().write(src);
      metrics.incContainerBytesStats(getType(), writeBytes);
      containerData.updateWriteStats(writeBytes, false);
      metrics.incContainerOpsLatencies(getType(), Time.monotonicNowNanos() - startTime);
      return writeBytes;
    } catch (IOException e) {
      checkVolume();
      throw e;
    }
  }

  public ContainerMetrics getMetrics() {
    return metrics;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" +
        "File=" + file.getAbsolutePath() +
        ", containerID=" + containerData.getContainerID() +
        '}';
  }
}

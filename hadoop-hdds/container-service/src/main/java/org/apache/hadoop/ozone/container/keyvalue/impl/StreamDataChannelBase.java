/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.impl;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.ratis.statemachine.StateMachine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * For write state machine data.
 */
abstract class StreamDataChannelBase implements StateMachine.DataChannel {
  private final RandomAccessFile randomAccessFile;

  private final File file;

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

  @Override
  public final void force(boolean metadata) throws IOException {
    getChannel().force(metadata);
  }

  @Override
  public final boolean isOpen() {
    return getChannel().isOpen();
  }

  @Override
  public void close() throws IOException {
    randomAccessFile.close();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    final int writeBytes = getChannel().write(src);
    metrics.incContainerBytesStats(getType(), writeBytes);
    containerData.updateWriteStats(writeBytes, false);
    return writeBytes;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" +
        "File=" + file.getAbsolutePath() +
        ", containerID=" + containerData.getContainerID() +
        '}';
  }
}

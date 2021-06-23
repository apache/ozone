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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import org.apache.ratis.statemachine.StateMachine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;

class StreamDataChannel implements StateMachine.DataChannel {
  private final Path path;
  private final RandomAccessFile randomAccessFile;

  StreamDataChannel(Path path) throws FileNotFoundException {
    this.path = path;
    this.randomAccessFile = new RandomAccessFile(path.toFile(), "rw");
  }

  @Override
  public void force(boolean metadata) throws IOException {
    randomAccessFile.getChannel().force(metadata);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return randomAccessFile.getChannel().write(src);
  }

  @Override
  public boolean isOpen() {
    return randomAccessFile.getChannel().isOpen();
  }

  @Override
  public void close() throws IOException {
    randomAccessFile.close();
  }
}

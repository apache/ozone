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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

class LocalStream implements StateMachine.DataStream {
  private final StateMachine.DataChannel dataChannel;

  LocalStream(StateMachine.DataChannel dataChannel) {
    this.dataChannel = dataChannel;
  }

  @Override
  public StateMachine.DataChannel getDataChannel() {
    return dataChannel;
  }

  @Override
  public CompletableFuture<?> cleanUp() {
    return CompletableFuture.supplyAsync(() -> {
      try {
        dataChannel.close();
        return true;
      } catch (IOException e) {
        throw new CompletionException("Failed to close data channel", e);
      }
    });
  }
}
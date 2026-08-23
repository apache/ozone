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

import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import org.apache.hadoop.ozone.container.keyvalue.impl.KeyValueStreamDataChannel;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.function.CheckedFunction;

class LocalStream implements StateMachine.DataStream {
  private final StateMachine.DataChannel dataChannel;
  private final Executor executor;
  private final CheckedFunction<ByteBuffer, ByteBuffer, IOException> commandHandler;

  LocalStream(StateMachine.DataChannel dataChannel, Executor executor,
      CheckedFunction<ByteBuffer, ByteBuffer, IOException> commandHandler) {
    this.dataChannel = dataChannel;
    this.executor = executor;
    this.commandHandler = commandHandler;
  }

  @Override
  public StateMachine.DataChannel getDataChannel() {
    return dataChannel;
  }

  @Override
  public CompletableFuture<?> cleanUp() {
    if (!(dataChannel instanceof KeyValueStreamDataChannel)) {
      return JavaUtils.completeExceptionally(new IllegalStateException(
          "Unexpected DataChannel " + dataChannel.getClass()));
    }
    return CompletableFuture
        .supplyAsync(((KeyValueStreamDataChannel) dataChannel)::cleanUp,
            executor);
  }

  @Override
  public Executor getExecutor() {
    return executor;
  }

  @Override
  public CompletableFuture<ByteBuffer> onCommand(ByteBuffer command, long streamOffset) {
    if (commandHandler == null) {
      return CompletableFuture.completedFuture(null);
    }
    final Executor exec = executor != null ? executor : Runnable::run;
    return CompletableFuture.supplyAsync(() -> {
      try {
        return commandHandler.apply(command);
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    }, exec);
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocolProtos.RequestType;

public class SCMStateMachine extends BaseStateMachine {

  private static Map<String, Class<?>> classCache = new HashMap<>();

  private final HashMap<RequestType, Object> handlers;

  public SCMStateMachine() {
    this.handlers = new HashMap<>();
  }

  @Override
  public void initialize(final RaftServer server, final RaftGroupId id,
                         final RaftStorage raftStorage) throws IOException {
    lifeCycle.startAndTransition(() -> super.initialize(server, id, raftStorage));
  }

  public void registerHandler(RequestType type, Object handler) {
    handlers.put(type, handler);
  }

  @Override
  public CompletableFuture<Message> applyTransaction(
      final TransactionContext trx) {
    final CompletableFuture<Message> applyTransactionFuture =
        new CompletableFuture<>();
    try {
      final SCMRatisRequest request = SCMRatisRequest.decode(
          trx.getClientRequest().getMessage());
      applyTransactionFuture.complete(process(request));
    } catch (Exception ex) {
      applyTransactionFuture.completeExceptionally(ex);
    }
    return applyTransactionFuture;
  }

  private Message process(final SCMRatisRequest request)
      throws Exception {
    try {
      final Object handler = handlers.get(request.getType());

      if (handler == null) {
        // No handler registered for the request type.
        throw new IOException("No handler found for request type " +
            request.getType());
      }

      final List<Class<?>> argumentTypes = new ArrayList<>();
      for(Object args : request.getArguments()) {
        argumentTypes.add(args.getClass());
      }
      final Object result = handler.getClass().getMethod(
          request.getOperation(), argumentTypes.toArray(new Class<?>[0]))
          .invoke(handler, request.getArguments());

      return SCMRatisResponse.encode(result);
    } catch (NoSuchMethodException | SecurityException ex) {
      throw new InvalidProtocolBufferException(ex.getMessage());
    } catch (InvocationTargetException e) {
      final Exception targetEx = (Exception) e.getTargetException();
      throw targetEx != null ? targetEx : e;
    }
  }

}

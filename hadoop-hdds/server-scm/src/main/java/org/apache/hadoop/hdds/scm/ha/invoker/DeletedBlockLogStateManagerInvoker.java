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

package org.apache.hadoop.hdds.scm.ha.invoker;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogStateManager;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * Invoker for DeletedBlockLogStateManager local (non-@Replicate) methods.
 */
public class DeletedBlockLogStateManagerInvoker extends ScmInvoker<DeletedBlockLogStateManager> {
  private final DeletedBlockLogStateManager impl;

  enum ReplicateMethod implements NameAndParameterTypes {
    addTransactionsToDB(new Class<?>[] {ArrayList.class, DeletedBlocksTransactionSummary.class}),
    removeTransactionsFromDB(new Class<?>[] {ArrayList.class, DeletedBlocksTransactionSummary.class}),;

    private final Class<?>[][] parameterTypes;

    ReplicateMethod(Class<?>[] parameterTypes) {
      final Class<?>[][] types = new Class<?>[parameterTypes.length + 1][];
      for (int i = 0; i <= parameterTypes.length; ++i) {
        types[i] = Arrays.copyOf(parameterTypes, i);
      }
      this.parameterTypes = types;
    }

    @Override
    public String getName() {
      return name();
    }

    @Override
    public Class<?>[] getParameterTypes(int numArgs) {
      return parameterTypes[numArgs];
    }
  }

  public DeletedBlockLogStateManagerInvoker(DeletedBlockLogStateManager impl, SCMRatisServer scmRatisServer) {
    super(scmRatisServer);
    this.impl = impl;
  }

  @Override
  public RequestType getType() {
    return RequestType.BLOCK;
  }

  @Override
  public Class<DeletedBlockLogStateManager> getApi() {
    return DeletedBlockLogStateManager.class;
  }

  @Override
  public DeletedBlockLogStateManager getImpl() {
    return impl;
  }

  @Override
  public DeletedBlockLogStateManager getProxy() {
    return new DeletedBlockLogStateManager() {
      @Override
      public void addTransactionsToDB(ArrayList<DeletedBlocksTransaction> txs)
          throws IOException {
        final Object[] args = {txs};
        invokeRatisServer(ReplicateMethod.addTransactionsToDB, args);
      }

      @Override
      public void addTransactionsToDB(ArrayList<DeletedBlocksTransaction> txs,
          DeletedBlocksTransactionSummary summary) throws IOException {
        final Object[] args = {txs, summary};
        invokeRatisServer(ReplicateMethod.addTransactionsToDB, args);
      }

      @Override
      public void removeTransactionsFromDB(ArrayList<Long> txIDs)
          throws IOException {
        final Object[] args = {txIDs};
        invokeRatisServer(ReplicateMethod.removeTransactionsFromDB, args);
      }

      @Override
      public void removeTransactionsFromDB(ArrayList<Long> txIDs,
          DeletedBlocksTransactionSummary summary) throws IOException {
        final Object[] args = {txIDs, summary};
        invokeRatisServer(ReplicateMethod.removeTransactionsFromDB, args);
      }

      @Override
      public Table.KeyValueIterator<Long, DeletedBlocksTransaction> getReadOnlyIterator() throws IOException {
        return impl.getReadOnlyIterator();
      }

      @Override
      public void onFlush() {
        impl.onFlush();
      }

      @Override
      public void reinitialize(
          Table<Long, DeletedBlocksTransaction> deletedBlocksTXTable,
          Table<String, ByteString> statefulConfigTable) {
        impl.reinitialize(deletedBlocksTXTable, statefulConfigTable);
      }
    };
  }

  // Code generated for DeletedBlockLogStateManager.  Do not modify.
  @SuppressWarnings("unchecked")
  @Override
  public Object invokeLocal(String methodName, Object[] params) throws Exception {
    switch (methodName) {
    case "onFlush":
      impl.onFlush();
      return null;

    case "addTransactionsToDB":
      final ArrayList arg0 =
          params.length > 0 ? (ArrayList) params[0] : null;
      final DeletedBlocksTransactionSummary arg1 =
          params.length > 1 ? (DeletedBlocksTransactionSummary) params[1] : null;
      impl.addTransactionsToDB(arg0, arg1);
      return null;

    case "removeTransactionsFromDB":
      final ArrayList arg2 =
          params.length > 0 ? (ArrayList) params[0] : null;
      final DeletedBlocksTransactionSummary arg3 =
          params.length > 1 ? (DeletedBlocksTransactionSummary) params[1] : null;
      impl.removeTransactionsFromDB(arg2, arg3);
      return null;

    case "getReadOnlyIterator":
      return impl.getReadOnlyIterator();

    case "reinitialize":
      final Table arg4 = params.length > 0 ? (Table) params[0] : null;
      final Table arg5 = params.length > 1 ? (Table) params[1] : null;
      impl.reinitialize(arg4, arg5);
      return null;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName);
    }
  }

}

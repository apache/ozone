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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogStateManager;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.utils.db.Table;

/** Code generated for {@link DeletedBlockLogStateManager}.  Do not modify. */
public class DeletedBlockLogStateManagerInvoker extends ScmInvoker<DeletedBlockLogStateManager> {
  enum ReplicateMethod implements NameAndParameterTypes {
    addTransactionsToDB(new Class<?>[][] {
        null,
        new Class<?>[] {ArrayList.class},
        new Class<?>[] {ArrayList.class, DeletedBlocksTransactionSummary.class}
    }),
    removeTransactionsFromDB(new Class<?>[][] {
        null,
        new Class<?>[] {ArrayList.class},
        new Class<?>[] {ArrayList.class, DeletedBlocksTransactionSummary.class}
    });

    private final Class<?>[][] parameterTypes;

    ReplicateMethod(Class<?>[][] parameterTypes) {
      this.parameterTypes = parameterTypes;
    }

    @Override
    public Class<?>[] getParameterTypes(int numArgs) {
      return parameterTypes[numArgs];
    }
  }

  public DeletedBlockLogStateManagerInvoker(DeletedBlockLogStateManager impl, SCMRatisServer ratis) {
    super(impl, DeletedBlockLogStateManagerInvoker::newProxy, ratis);
  }

  @Override
  public Class<DeletedBlockLogStateManager> getApi() {
    return DeletedBlockLogStateManager.class;
  }

  static DeletedBlockLogStateManager newProxy(ScmInvoker<DeletedBlockLogStateManager> invoker) {
    return new DeletedBlockLogStateManager() {

      @Override
      public void addTransactionsToDB(ArrayList arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.addTransactionsToDB, args);
      }

      @Override
      public void addTransactionsToDB(ArrayList arg0, DeletedBlocksTransactionSummary arg1) throws IOException {
        final Object[] args = {arg0, arg1};
        invoker.invokeReplicateDirect(ReplicateMethod.addTransactionsToDB, args);
      }

      @Override
      public Table.KeyValueIterator<Long, DeletedBlocksTransaction> getReadOnlyIterator() throws IOException {
        return invoker.getImpl().getReadOnlyIterator();
      }

      @Override
      public void onFlush() {
        invoker.getImpl().onFlush();
      }

      @Override
      public void reinitialize(Table arg0, Table arg1) {
        invoker.getImpl().reinitialize(arg0, arg1);
      }

      @Override
      public void removeTransactionsFromDB(ArrayList arg0) throws IOException {
        final Object[] args = {arg0};
        invoker.invokeReplicateDirect(ReplicateMethod.removeTransactionsFromDB, args);
      }

      @Override
      public void removeTransactionsFromDB(ArrayList arg0, DeletedBlocksTransactionSummary arg1) throws IOException {
        final Object[] args = {arg0, arg1};
        invoker.invokeReplicateDirect(ReplicateMethod.removeTransactionsFromDB, args);
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object invokeLocal(String methodName, Object[] p) throws Exception {
    switch (methodName) {
    case "addTransactionsToDB":
      final ArrayList arg0 = p.length > 0 ? (ArrayList) p[0] : null;
      final DeletedBlocksTransactionSummary arg1 = p.length > 1 ? (DeletedBlocksTransactionSummary) p[1] : null;
      getImpl().addTransactionsToDB(arg0, arg1);
      return null;

    case "getReadOnlyIterator":
      return getImpl().getReadOnlyIterator();

    case "onFlush":
      getImpl().onFlush();
      return null;

    case "reinitialize":
      final Table arg2 = p.length > 0 ? (Table) p[0] : null;
      final Table arg3 = p.length > 1 ? (Table) p[1] : null;
      getImpl().reinitialize(arg2, arg3);
      return null;

    case "removeTransactionsFromDB":
      final ArrayList arg4 = p.length > 0 ? (ArrayList) p[0] : null;
      final DeletedBlocksTransactionSummary arg5 = p.length > 1 ? (DeletedBlocksTransactionSummary) p[1] : null;
      getImpl().removeTransactionsFromDB(arg4, arg5);
      return null;

    default:
      throw new IllegalArgumentException("Method not found: " + methodName + " in DeletedBlockLogStateManager");
    }
  }

  @Override
  public Class<?> getReturnType(String methodName, Class<?>[] parameterTypes) {
    switch (methodName) {
    case "addTransactionsToDB":
      if (Arrays.equals(parameterTypes, new Class<?>[]{ArrayList.class})) {
        return void.class;
      }
      if (Arrays.equals(parameterTypes, new Class<?>[]{ArrayList.class, DeletedBlocksTransactionSummary.class})) {
        return void.class;
      }
      break;

    case "getReadOnlyIterator":
      if (parameterTypes == null || parameterTypes.length == 0) {
        return Table.KeyValueIterator.class;
      }
      break;

    case "onFlush":
      if (parameterTypes == null || parameterTypes.length == 0) {
        return void.class;
      }
      break;

    case "reinitialize":
      if (Arrays.equals(parameterTypes, new Class<?>[]{Table.class, Table.class})) {
        return void.class;
      }
      break;

    case "removeTransactionsFromDB":
      if (Arrays.equals(parameterTypes, new Class<?>[]{ArrayList.class})) {
        return void.class;
      }
      if (Arrays.equals(parameterTypes, new Class<?>[]{ArrayList.class, DeletedBlocksTransactionSummary.class})) {
        return void.class;
      }
      break;

    default:
      break;
    }

    throw new IllegalArgumentException(
        "Method not found: " + methodName
            + " with parameterTypes in DeletedBlockLogStateManager");
  }
}

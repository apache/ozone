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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogStateManager;
import org.apache.hadoop.hdds.scm.ha.ScmInvoker;
import org.apache.hadoop.hdds.utils.db.Table;
import java.util.ArrayList;

/**
 * Invoker for DeletedBlockLogStateManager local (non-@Replicate) methods.
 */
public class DeletedBlockLogStateManagerInvoker implements ScmInvoker<DeletedBlockLogStateManager> {
  private final DeletedBlockLogStateManager impl;

  public DeletedBlockLogStateManagerInvoker(DeletedBlockLogStateManager impl) {
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

  // Code generated for DeletedBlockLogStateManager.  Do not modify.
  @SuppressWarnings("unchecked")
  @Override
  public Object invokeLocal(String methodName, Object[] params) throws Exception {
    switch (methodName) {
      case "onFlush": {
        impl.onFlush();
        return null;
      }
      case "addTransactionsToDB": {
        final ArrayList arg0 = params.length > 0 ? (ArrayList) params[0] : null;
        final DeletedBlocksTransactionSummary
            arg1 = params.length > 1 ? (DeletedBlocksTransactionSummary) params[1] : null;
        impl.addTransactionsToDB(arg0, arg1);
        return null;
      }
      case "removeTransactionsFromDB": {
        final ArrayList arg0 = params.length > 0 ? (ArrayList) params[0] : null;
        final DeletedBlocksTransactionSummary
            arg1 = params.length > 1 ? (DeletedBlocksTransactionSummary) params[1] : null;
        impl.removeTransactionsFromDB(arg0, arg1);
        return null;
      }
      case "getReadOnlyIterator": {
        return impl.getReadOnlyIterator();
      }
      case "reinitialize": {
        final Table arg0 = params.length > 0 ? (Table) params[0] : null;
        final Table arg1 = params.length > 1 ? (Table) params[1] : null;
        impl.reinitialize(arg0, arg1);
        return null;
      }
    }
    throw new IllegalArgumentException("Method not found: " + methodName);
  }

  @SuppressWarnings("unchecked")
  private Table<Long,
      StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction> castDeletedBlocksTable(Object arg) {
    return (Table<Long, StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction>) arg;
  }

  @SuppressWarnings("unchecked")
  private Table<String, ByteString> castStatefulConfigTable(Object arg) {
    return (Table<String, ByteString>) arg;
  }
}

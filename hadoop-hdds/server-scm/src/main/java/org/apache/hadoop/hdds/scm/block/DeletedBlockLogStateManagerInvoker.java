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

package org.apache.hadoop.hdds.scm.block;

import com.google.protobuf.ByteString;
import java.lang.reflect.Method;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvoker;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * Invoker for DeletedBlockLogStateManager local (non-@Replicate) methods.
 */
public class DeletedBlockLogStateManagerInvoker implements SCMHAInvoker<DeletedBlockLogStateManager> {

  @Override
  public Object invoke(DeletedBlockLogStateManager handler,
      Method method, Object[] args) throws Exception {
    switch (method.getName()) {
    case "getReadOnlyIterator":
      return handler.getReadOnlyIterator();

    case "onFlush":
      handler.onFlush();
      return null;

    case "reinitialize":
      handler.reinitialize(
          castDeletedBlocksTable(args[0]),
          castStatefulConfigTable(args[1]));
      return null;

    default:
      throw new UnsupportedOperationException(
          "Unsupported method: " + method.getName());
    }
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

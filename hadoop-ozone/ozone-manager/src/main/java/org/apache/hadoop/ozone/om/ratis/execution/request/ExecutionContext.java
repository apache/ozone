/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.ratis.execution.request;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * context required for execution of request.
 */
public class ExecutionContext {
  private long index;
  private TermIndex termIndex;
  private BatchOperation batchOperation;

  public long getIndex() {
    return index;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  public TermIndex getTermIndex() {
    if (null == termIndex) {
      return TermIndex.valueOf(-1, index);
    }
    return termIndex;
  }

  public void setTermIndex(TermIndex termIndex) {
    this.termIndex = termIndex;
  }

  public void setBatchOperation(BatchOperation batchOperation) {
    this.batchOperation = batchOperation;
  }

  public BatchOperation getBatchOperation() {
    return batchOperation;
  }
}

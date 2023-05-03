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

import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.ratis.statemachine.SnapshotInfo;

import java.io.IOException;

/**
 * DB transaction that buffers SCM DB transactions. Call the flush method
 * to flush a batch into SCM DB. This buffer also maintains a latest transaction
 * info to indicate the information of the latest transaction in the buffer.
 */
public interface SCMHADBTransactionBuffer
    extends DBTransactionBuffer {

  void updateLatestTrxInfo(TransactionInfo info);

  TransactionInfo getLatestTrxInfo();

  SnapshotInfo getLatestSnapshot();

  void setLatestSnapshot(SnapshotInfo latestSnapshot);

  void flush() throws IOException;

  void init() throws IOException;
}

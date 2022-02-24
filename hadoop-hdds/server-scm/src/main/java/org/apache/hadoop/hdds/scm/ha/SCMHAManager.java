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

import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.ratis.server.protocol.TermIndex;

import java.io.IOException;

/**
 * SCMHAManager provides HA service for SCM.
 */
public interface SCMHAManager {

  /**
   * Starts HA service.
   */
  void start() throws IOException;

  /**
   * Returns RatisServer instance associated with the SCM instance.
   */
  SCMRatisServer getRatisServer();

  /**
   * Returns SCM snapshot provider.
   */
  SCMSnapshotProvider getSCMSnapshotProvider();

  /**
   * Returns DB transaction buffer.
   */
  DBTransactionBuffer getDBTransactionBuffer();

  /**
   * Returns the DBTransactionBuffer as SCMHADBTransactionBuffer if its
   * valid.
   * @return
   */
  SCMHADBTransactionBuffer asSCMHADBTransactionBuffer();

  /**
   * Stops the HA service.
   */
  void shutdown() throws IOException;

  /**
   * Adds the SC M instance to the SCM HA group.
   * @param request AddSCM request
   * @return status signying whether the AddSCM request succeeded or not.
   * @throws IOException
   */
  boolean addSCM(AddSCMRequest request) throws IOException;

  /**
   * Download the latest checkpoint from leader SCM.
   *
   * @param leaderId peerNodeID of the leader SCM
   * @return If checkpoint is installed successfully, return the
   *         corresponding termIndex. Otherwise, return null.
   */
  DBCheckpoint downloadCheckpointFromLeader(String leaderId);

  /**
   * Verify the SCM DB checkpoint downloaded from leader.
   *
   * @param leaderId : leaderId
   * @param checkpoint : checkpoint downloaded from leader.
   * @return If the checkpoints snapshot index is greater than SCM's
   *         last applied transaction index, return the termIndex of
   *         the checkpoint, otherwise return null.
   */
  TermIndex verifyCheckpointFromLeader(String leaderId,
                                       DBCheckpoint checkpoint);

  /**
   * Re-initialize the SCM state via this checkpoint.
   */
  TermIndex installCheckpoint(DBCheckpoint dbCheckpoint) throws Exception;
}

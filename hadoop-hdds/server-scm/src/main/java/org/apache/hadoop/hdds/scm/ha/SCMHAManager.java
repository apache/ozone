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

package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.RemoveSCMRequest;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * SCMHAManager provides HA service for SCM.
 */
public interface SCMHAManager extends AutoCloseable {

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
   */
  SCMHADBTransactionBuffer asSCMHADBTransactionBuffer();

  /**
   * Stops the HA service.
   */
  void stop() throws IOException;

  /**
   * Adds the SCM instance to the SCM HA Ring.
   * @param request AddSCM request
   * @return status signying whether the AddSCM request succeeded or not.
   * @throws IOException
   */
  boolean addSCM(AddSCMRequest request) throws IOException;

  /** Remove the SCM instance from the SCM HA Ring.
   * @param request RemoveSCM request
   *
   * @return status signaling whether the RemoveSCM request succeeded or not.
   * @throws IOException
   */
  boolean removeSCM(RemoveSCMRequest request) throws IOException;

  /**
   * Download the latest checkpoint from leader SCM.
   *
   * @param leaderId peerNodeID of the leader SCM
   * @return If checkpoint is installed successfully, return the
   *         corresponding termIndex. Otherwise, return null.
   */
  DBCheckpoint downloadCheckpointFromLeader(String leaderId);

  /**
   * Get secret keys from SCM leader.
   */
  List<ManagedSecretKey> getSecretKeysFromLeader(String leaderID)
      throws IOException;

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

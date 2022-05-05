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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType.MOVE;

/**
 * Ratis based MoveScheduler, db operations are stored in
 * DBTransactionBuffer until a snapshot is taken.
 */
public final class MoveSchedulerImpl implements MoveScheduler {
  public static final Logger LOG =
      LoggerFactory.getLogger(MoveSchedulerImpl.class);

  private Table<ContainerID, MoveDataNodePair> moveTable;
  private final DBTransactionBuffer transactionBuffer;

  /**
   * This is used for tracking container move commands
   * which are not yet complete, and reply to client when
   * complete or failed. note that, this will not be stored
   * in db
   */
  private final Map<ContainerID,
      CompletableFuture<MoveResult>> inflightMoveFuture;

  /**
   * This is used for tracking container move commands
   * which are not yet complete. note that, this will be stored
   * in db.
   */
  private final Map<ContainerID, MoveDataNodePair> inflightMove;

  private MoveSchedulerImpl(
      Table<ContainerID, MoveDataNodePair> moveTable,
      DBTransactionBuffer transactionBuffer) throws IOException {
    this.moveTable = moveTable;
    this.transactionBuffer = transactionBuffer;
    this.inflightMove = new ConcurrentHashMap<>();
    this.inflightMoveFuture = new ConcurrentHashMap<>();
    initialize();
  }

  @Override
  public void completeMove(HddsProtos.ContainerID contianerIDProto) {
    ContainerID cid = null;
    try {
      cid = ContainerID.getFromProtobuf(contianerIDProto);
      transactionBuffer.removeFromBuffer(moveTable, cid);
    } catch (IOException e) {
      LOG.warn("Exception while completing move {}", cid);
    }
    inflightMove.remove(cid);
  }

  @Override
  public void startMove(HddsProtos.ContainerID contianerIDProto,
                        HddsProtos.MoveDataNodePairProto mdnpp)
      throws IOException {
    ContainerID cid = null;
    MoveDataNodePair mp;
    try {
      cid = ContainerID.getFromProtobuf(contianerIDProto);
      mp = MoveDataNodePair.getFromProtobuf(mdnpp);
      if (!inflightMove.containsKey(cid)) {
        transactionBuffer.addToBuffer(moveTable, cid, mp);
        inflightMove.putIfAbsent(cid, mp);
      }
    } catch (IOException e) {
      LOG.warn("Exception while completing move {}", cid);
    }
  }

  @Override
  public void addMoveCompleteFuture(
      ContainerID cid, CompletableFuture<MoveResult> moveFuture) {
    inflightMoveFuture.putIfAbsent(cid, moveFuture);
  }

  /**
   * complete the CompletableFuture of the container in the given Map with
   * a given MoveResult.
   */
  @Override
  public void compleleteMoveFutureWithResult(ContainerID cid, MoveResult mr) {
    if (inflightMoveFuture.containsKey(cid)) {
      inflightMoveFuture.get(cid).complete(mr);
      inflightMoveFuture.remove(cid);
    }
  }

  @Override
  public MoveDataNodePair getMoveDataNodePair(ContainerID cid) {
    return inflightMove.get(cid);
  }

  @Override
  public void reinitialize(Table<ContainerID,
      MoveDataNodePair> mt) throws IOException {
    moveTable = mt;
    inflightMove.clear();
    inflightMoveFuture.clear();
    initialize();
  }

  private void initialize() throws IOException {
    TableIterator<ContainerID,
        ? extends Table.KeyValue<ContainerID, MoveDataNodePair>>
        iterator = moveTable.iterator();

    while (iterator.hasNext()) {
      Table.KeyValue<ContainerID, MoveDataNodePair> kv = iterator.next();
      final ContainerID cid = kv.getKey();
      final MoveDataNodePair mp = kv.getValue();
      Preconditions.assertNotNull(cid,
          "moved container id should not be null");
      Preconditions.assertNotNull(mp,
          "MoveDataNodePair container id should not be null");
      inflightMove.put(cid, mp);
    }
  }

  @Override
  public Map<ContainerID, MoveDataNodePair> getInflightMove() {
    return inflightMove;
  }

  /**
   * Builder for Ratis based MoveSchedule.
   */
  public static class Builder {
    private Table<ContainerID, MoveDataNodePair> moveTable;
    private DBTransactionBuffer transactionBuffer;
    private SCMRatisServer ratisServer;

    public Builder setRatisServer(final SCMRatisServer scmRatisServer) {
      ratisServer = scmRatisServer;
      return this;
    }

    public Builder setMoveTable(
        final Table<ContainerID, MoveDataNodePair> mt) {
      moveTable = mt;
      return this;
    }

    public Builder setDBTransactionBuffer(DBTransactionBuffer trxBuffer) {
      transactionBuffer = trxBuffer;
      return this;
    }

    public MoveScheduler build() throws IOException {
      Preconditions.assertNotNull(moveTable, "moveTable is null");
      Preconditions.assertNotNull(transactionBuffer,
          "transactionBuffer is null");

      final MoveScheduler impl =
          new MoveSchedulerImpl(moveTable, transactionBuffer);

      final SCMHAInvocationHandler invocationHandler
          = new SCMHAInvocationHandler(MOVE, impl, ratisServer);

      return (MoveScheduler) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{MoveScheduler.class},
          invocationHandler);
    }
  }
}

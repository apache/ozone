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

package org.apache.hadoop.hdds.utils;

import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * Represents information about a flushed transaction, including its term and transaction index.
 * This class is a lightweight container used to track specific transaction metadata and provides
 * methods for serialization and deserialization using a codec.
 */
public class FlushedTransactionInfo {

  private static final Codec<FlushedTransactionInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(HddsProtos.FlushedTransactionInfo.getDefaultInstance()),
      FlushedTransactionInfo::getFromProtobuf,
      FlushedTransactionInfo::getProtobuf,
      FlushedTransactionInfo.class);

  private final long term;
  private final long transactionIndex;

  FlushedTransactionInfo(TermIndex termIndex) {
    this.transactionIndex = termIndex.getIndex();
    this.term = termIndex.getTerm();
  }

  public static FlushedTransactionInfo valueOf(long currentTerm, long transactionIndex) {
    return valueOf(TermIndex.valueOf(currentTerm, transactionIndex));
  }

  public static FlushedTransactionInfo valueOf(TermIndex termIndex) {
    return new FlushedTransactionInfo(termIndex);
  }

  public static Codec<FlushedTransactionInfo> getCodec() {
    return CODEC;
  }

  public long getTerm() {
    return term;
  }

  public long getTransactionIndex() {
    return transactionIndex;
  }

  public static FlushedTransactionInfo getFromProtobuf(HddsProtos.FlushedTransactionInfo transactionInfo) {
    return new FlushedTransactionInfo(TermIndex.valueOf(transactionInfo.getTermIndex(),
        transactionInfo.getTransactionId()));
  }

  private HddsProtos.FlushedTransactionInfo getProtobuf() {
    return HddsProtos.FlushedTransactionInfo.newBuilder().setTermIndex(this.getTerm())
        .setTransactionId(this.getTransactionIndex()).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FlushedTransactionInfo that = (FlushedTransactionInfo) o;
    return this.getTerm() == that.getTerm() && this.getTransactionIndex() == that.getTransactionIndex();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTerm(), getTransactionIndex());
  }

  @Override
  public String toString() {
    return "FlushedTransactionInfo{" +
        "term=" + term +
        ", transactionIndex=" + transactionIndex +
        '}';
  }
}

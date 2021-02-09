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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.ozone.common.ha.ratis.RatisSnapshotInfo;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SnapshotInfo;

import java.util.Objects;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_SPLIT_KEY;

/**
 * SCMTransactionInfo saves two fields for a transaction:
 *  1. term, of which the transaction belongs to
 *  2. transactionIndex, which is a monotonic increasing index
 *     (e.g. Raft Log index)
 */
public final class SCMTransactionInfo {
  private long term;
  private long transactionIndex;

  private SCMTransactionInfo(String transactionInfo) {
    String[] tInfo =
        transactionInfo.split(TRANSACTION_INFO_SPLIT_KEY);
    Preconditions.checkState(tInfo.length == 2,
        "Incorrect TransactionInfo value");

    term = Long.parseLong(tInfo[0]);
    transactionIndex = Long.parseLong(tInfo[1]);
  }

  private SCMTransactionInfo(long currentTerm, long transactionIndex) {
    this.term = currentTerm;
    this.transactionIndex = transactionIndex;
  }

  public boolean isInitialized() {
    return transactionIndex == -1 && term == 0;
  }

  public int compareTo(SCMTransactionInfo info) {
    if (info.getTerm() == this.getTerm()) {
      return this.getTransactionIndex() <= info.getTransactionIndex() ? -1 : 1;
    } else {
      return this.getTerm() < info.getTerm() ? -1 : 1;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SCMTransactionInfo that = (SCMTransactionInfo) o;
    return term == that.term &&
        transactionIndex == that.transactionIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, transactionIndex);
  }

  @Override
  public String toString() {
    return "[" + term + ":" + transactionIndex + "]";
  }

  public long getTerm() {
    return term;
  }

  public long getTransactionIndex() {
    return transactionIndex;
  }

  public static SCMTransactionInfo fromTermIndex(TermIndex termIndex) {
    return builder().setCurrentTerm(termIndex.getTerm())
        .setTransactionIndex(termIndex.getIndex()).build();
  }

  public SnapshotInfo toSnapshotInfo() {
    return new RatisSnapshotInfo(term, transactionIndex);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private long currentTerm = 0;
    private long transactionIndex = -1;

    public Builder setCurrentTerm(long term) {
      this.currentTerm = term;
      return this;
    }

    public Builder setTransactionIndex(long tIndex) {
      this.transactionIndex = tIndex;
      return this;
    }

    public SCMTransactionInfo build() {
      return new SCMTransactionInfo(currentTerm, transactionIndex);
    }
  }

  public static SCMTransactionInfo getFromByteArray(byte[] bytes) {
    String tInfo = StringUtils.bytes2String(bytes);
    return new SCMTransactionInfo(tInfo);
  }

  public byte[] convertToByteArray() {
    return StringUtils.string2Bytes(generateTransactionInfo());
  }

  private String generateTransactionInfo() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(term);
    stringBuilder.append(TRANSACTION_INFO_SPLIT_KEY);
    stringBuilder.append(transactionIndex);

    return stringBuilder.toString();
  }
}

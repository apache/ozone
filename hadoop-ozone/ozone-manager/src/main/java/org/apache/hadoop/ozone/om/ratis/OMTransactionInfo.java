/*
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

package org.apache.hadoop.ozone.om.ratis;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.StringUtils;

import java.util.Objects;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_SPLIT_KEY;

/**
 * TransactionInfo which is persisted to OM DB.
 */
public final class OMTransactionInfo {

  private long currentTerm; // term associated with the ratis log index.
  // Transaction index corresponds to ratis log index
  private long transactionIndex;

  private OMTransactionInfo(String transactionInfo) {
    String[] tInfo =
        transactionInfo.split(TRANSACTION_INFO_SPLIT_KEY);
    Preconditions.checkState(tInfo.length==2,
        "Incorrect TransactionInfo value");

    currentTerm = Long.parseLong(tInfo[0]);
    transactionIndex = Long.parseLong(tInfo[1]);
  }

  private OMTransactionInfo(long currentTerm, long transactionIndex) {
    this.currentTerm = currentTerm;
    this.transactionIndex = transactionIndex;
  }

  /**
   * Get current term.
   * @return currentTerm
   */
  public long getCurrentTerm() {
    return currentTerm;
  }

  /**
   * Get current transaction index.
   * @return transactionIndex
   */
  public long getTransactionIndex() {
    return transactionIndex;
  }

  /**
   * Generate String form of transaction info which need to be persisted in OM
   * DB finally in byte array.
   * @return transaction info.
   */
  private String generateTransactionInfo() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(currentTerm);
    stringBuilder.append(TRANSACTION_INFO_SPLIT_KEY);
    stringBuilder.append(transactionIndex);

    return stringBuilder.toString();
  }

  /**
   * Convert OMTransactionInfo to byteArray to be persisted to OM DB.
   * @return byte[]
   */
  public byte[] convertToByteArray() {
    return StringUtils.string2Bytes(generateTransactionInfo());
  }

  /**
   * Convert byte array persisted in DB to OMTransactionInfo.
   * @param bytes
   * @return OMTransactionInfo
   */
  public static OMTransactionInfo getFromByteArray(byte[] bytes) {
    String tInfo = StringUtils.bytes2String(bytes);
    return new OMTransactionInfo(tInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OMTransactionInfo that = (OMTransactionInfo) o;
    return currentTerm == that.currentTerm &&
        transactionIndex == that.transactionIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(currentTerm, transactionIndex);
  }

  /**
   * Builder to build {@link OMTransactionInfo}.
   */
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

    public OMTransactionInfo build() {
      return new OMTransactionInfo(currentTerm, transactionIndex);
    }

  }
}

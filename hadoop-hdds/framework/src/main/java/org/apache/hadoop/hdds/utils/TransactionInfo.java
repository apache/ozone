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

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_SPLIT_KEY;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;

/**
 * TransactionInfo which is persisted to DB.
 * <p>
 * This class is immutable.
 */
public final class TransactionInfo implements Comparable<TransactionInfo> {
  private static final Codec<TransactionInfo> CODEC = new DelegatedCodec<>(
      StringCodec.get(),
      TransactionInfo::valueOf,
      TransactionInfo::toString,
      TransactionInfo.class,
      DelegatedCodec.CopyType.SHALLOW);

  public static final TransactionInfo DEFAULT_VALUE = valueOf(0, -1);

  /** In non-Ratis clusters, term is -1. */
  public static final long NON_RATIS_TERM = -1;

  /**
   * Use {@link SnapshotInfo} to store (term, index)
   * which is the Ratis Log term-index in Ratis enabled cluster.
   * In non-Ratis clusters, term is -1 and index is the unique transaction index
   * in OzoneManagerProtocolServerSideTranslatorPB#transactionIndex.
   */
  private final SnapshotInfo snapshotInfo;

  /** The string need to be persisted in OM DB. */
  private final String transactionInfoString;

  public static Codec<TransactionInfo> getCodec() {
    return CODEC;
  }

  public static TransactionInfo valueOf(String transactionInfo) {
    final String[] tInfo = transactionInfo.split(TRANSACTION_INFO_SPLIT_KEY);
    Preconditions.checkArgument(tInfo.length == 2,
        "Unexpected split length: %s in \"%s\"", tInfo.length, transactionInfo);

    try {
      return valueOf(Long.parseLong(tInfo[0]), Long.parseLong(tInfo[1]));
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse " + transactionInfo, e);
    }
  }

  public static TransactionInfo valueOf(long currentTerm, long transactionIndex) {
    return valueOf(TermIndex.valueOf(currentTerm, transactionIndex));
  }

  public static TransactionInfo valueOf(TermIndex termIndex) {
    return new TransactionInfo(termIndex);
  }

  public boolean isDefault() {
    return equals(DEFAULT_VALUE);
  }

  @Override
  public int compareTo(TransactionInfo info) {
    return this.getTermIndex().compareTo(info.getTermIndex());
  }

  /** For non-Ratis case. */
  public static TermIndex getTermIndex(long transactionIndex) {
    return TermIndex.valueOf(NON_RATIS_TERM, transactionIndex);
  }

  private TransactionInfo(TermIndex termIndex) {
    this.transactionInfoString = termIndex.getTerm() + TRANSACTION_INFO_SPLIT_KEY + termIndex.getIndex();
    this.snapshotInfo = new SnapshotInfo() {
      @Override
      public TermIndex getTermIndex() {
        return termIndex;
      }

      @Override
      public List<FileInfo> getFiles() {
        return null;
      }

      @Override
      public String toString() {
        return transactionInfoString;
      }
    };
  }

  /** @return Ratis Log term in Ratis enabled cluster; or -1 for non-Ratis clusters. */
  public long getTerm() {
    return snapshotInfo.getTerm();
  }

  /**
   * @return Ratis Log index in Ratis enabled cluster. For non-Ratis clusters, return the unique transaction index;
   *         see OzoneManagerProtocolServerSideTranslatorPB#transactionIndex.
   */
  public long getTransactionIndex() {
    return snapshotInfo.getIndex();
  }

  public TermIndex getTermIndex() {
    return snapshotInfo.getTermIndex();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TransactionInfo that = (TransactionInfo) o;
    return this.getTermIndex().equals(that.getTermIndex());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTerm(), getTransactionIndex());
  }

  @Override
  public String toString() {
    return transactionInfoString;
  }

  /**
   * Return transaction info persisted in OM DB.
   */
  public static TransactionInfo readTransactionInfo(
      DBStoreHAManager metadataManager) throws IOException {
    return metadataManager.getTransactionInfoTable().getSkipCache(TRANSACTION_INFO_KEY);
  }

  public ByteString toByteString() throws IOException {
    return ByteString.copyFrom(getCodec().toPersistedFormat(this));
  }

  public static TransactionInfo fromByteString(ByteString byteString) throws IOException {
    return byteString == null ? null : getCodec().fromPersistedFormat(byteString.toByteArray());
  }

  public SnapshotInfo toSnapshotInfo() {
    return snapshotInfo;
  }
}

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

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;

import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_SPLIT_KEY;

/**
 * This class captures the snapshotIndex and term of the latest snapshot in
 * the SCM
 * Ratis server loads the snapshotInfo during startup and updates the
 * lastApplied index to this snapshotIndex. SCM SnapshotInfo does not contain
 * any files. It is used only to store/ update the last applied index and term.
 */
public class SCMRatisSnapshotInfo implements SnapshotInfo {
  private final long term;
  private final long snapshotIndex;

  public SCMRatisSnapshotInfo(long term, long index) {
    this.term = term;
    this.snapshotIndex = index;
  }

  @Override
  public TermIndex getTermIndex() {
    return TermIndex.valueOf(term, snapshotIndex);
  }

  @Override
  public long getTerm() {
    return term;
  }

  @Override
  public long getIndex() {
    return snapshotIndex;
  }

  @Override
  public List<FileInfo> getFiles() {
    return null;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(term);
    stringBuilder.append(TRANSACTION_INFO_SPLIT_KEY);
    stringBuilder.append(snapshotIndex);
    return stringBuilder.toString();
  }
}

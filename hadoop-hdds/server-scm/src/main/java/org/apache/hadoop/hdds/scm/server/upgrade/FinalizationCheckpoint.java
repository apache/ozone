/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.server.upgrade;

/**
 * A finalization checkpoint is an abstraction over SCM's disk state,
 * indicating where finalization left off so it can be resumed on leader
 * change or restart. Currently the checkpoint is derived from two properties:
 * 1. The presence of a finalizing key in the database to indicate that
 * finalization is in progress.
 * 2. Whether SCM's metadata layout version is less than its software
 * layout version.
 */
public enum FinalizationCheckpoint {
  FINALIZATION_REQUIRED(false, true),
  FINALIZATION_STARTED(true, true),
  MLV_EQUALS_SLV(true, false),
  FINALIZATION_COMPLETE(false, false);

  private final boolean needsFinalizingMark;
  private final boolean needsMlvBehindSlv;

  FinalizationCheckpoint(boolean needsFinalizingMark,
                         boolean needsMlvBehindSlv) {
    this.needsFinalizingMark = needsFinalizingMark;
    this.needsMlvBehindSlv = needsMlvBehindSlv;
  }

  /**
   * Given external state, determines whether that corresponds to this
   * checkpoint.
   *
   * @param hasFinalizationMark true if finalization mark is present in the
   *                            DB.
   * @param hasMlvBehindSlv     true if the metadata layout version is less
   *                            than the software layout version
   * @return true if the provided state corresponds to this checkpoint.
   * False otherwise.
   */
  public boolean isCurrent(boolean hasFinalizationMark,
                           boolean hasMlvBehindSlv) {
    return hasFinalizationMark == needsFinalizingMark &&
        hasMlvBehindSlv == needsMlvBehindSlv;
  }

  public boolean needsFinalizingMark() {
    return needsFinalizingMark;
  }

  public boolean needsMlvBehindSlv() {
    return needsMlvBehindSlv;
  }

  public boolean hasCrossed(FinalizationCheckpoint query) {
    return this.compareTo(query) >= 0;
  }
}

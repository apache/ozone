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
package org.apache.ozone.rocksdiff;

import java.util.Map;

/**
 * Snapshot information node class for the differ.
 */
public class DifferSnapshotInfo {
  private final String dbPath;
  private final String snapshotID;
  private final long snapshotGeneration;

  private final Map<String, String> tablePrefixes;

  public DifferSnapshotInfo(String db, String id, long gen,
                            Map<String, String> prefixes) {
    dbPath = db;
    snapshotID = id;
    snapshotGeneration = gen;
    tablePrefixes = prefixes;
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getSnapshotID() {
    return snapshotID;
  }

  public long getSnapshotGeneration() {
    return snapshotGeneration;
  }

  public Map<String, String> getTablePrefixes() {
    return tablePrefixes;
  }

  @Override
  public String toString() {
    return String.format("DifferSnapshotInfo{dbPath='%s', snapshotID='%s', " +
                    "snapshotGeneration=%d, tablePrefixes size=%s}",
            dbPath, snapshotID, snapshotGeneration, tablePrefixes.size());
  }

}

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

package org.apache.ozone.rocksdiff;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import com.google.common.annotations.VisibleForTesting;
import java.nio.file.Path;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ozone.rocksdb.util.SstFileInfo;

/**
 * Snapshot information node class for the differ.
 */
public class DifferSnapshotInfo {
  private final UUID id;
  private final long generation;
  private final Function<Integer, Path> dbPathFunction;
  private final NavigableMap<Integer, List<SstFileInfo>> versionSstFiles;

  public DifferSnapshotInfo(Function<Integer, Path> dbPathFunction, UUID id, long gen,
                            NavigableMap<Integer, List<SstFileInfo>> sstFiles) {
    this.dbPathFunction = dbPathFunction;
    this.id = id;
    generation = gen;
    this.versionSstFiles = sstFiles;
  }

  public Path getDbPath(int version) {
    return dbPathFunction.apply(version);
  }

  public UUID getId() {
    return id;
  }

  public long getGeneration() {
    return generation;
  }

  List<SstFileInfo> getSstFiles(int version, Set<String> tablesToLookup) {
    return versionSstFiles.get(version).stream()
        .filter(sstFileInfo -> tablesToLookup.contains(sstFileInfo.getColumnFamily()))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  SstFileInfo getSstFile(int version, String fileName) {
    return versionSstFiles.get(version).stream()
        .filter(sstFileInfo -> sstFileInfo.getFileName().equals(fileName))
        .findFirst().orElse(null);
  }

  Integer getMaxVersion() {
    return versionSstFiles.lastKey();
  }

  @Override
  public String toString() {
    return String.format("DifferSnapshotInfo{dbPath='%s', id='%s', generation=%d}",
        versionSstFiles.keySet().stream().collect(toMap(identity(), dbPathFunction::apply)), id, generation);
  }
}

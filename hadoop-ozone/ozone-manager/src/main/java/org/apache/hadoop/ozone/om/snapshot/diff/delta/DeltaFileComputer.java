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

package org.apache.hadoop.ozone.om.snapshot.diff.delta;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ozone.rocksdb.util.SstFileInfo;

/**
 * The DeltaFileComputer interface defines a contract for computing delta files
 * that represent changes between two snapshots. Implementations of this
 * interface are responsible for determining the modifications made from a
 * baseline snapshot to a target snapshot in the form of delta files.
 */
public interface DeltaFileComputer extends Closeable {

  /**
   * Retrieves the delta files representing changes between two snapshots for specified tables.
   *
   * @param fromSnapshot the baseline snapshot from which changes are computed
   * @param toSnapshot the target snapshot to which changes are compared
   * @param tablesToLookup the set of table names to consider when determining changes
   * @return a collection of pairs, where each pair consists of a
   *         {@code Path} representing the delta file and an associated {@code SstFileInfo}, or
   *         an empty {@code Optional} if no changes are found
   * @throws IOException if an I/O error occurs while retrieving delta files
   */
  Collection<Pair<Path, SstFileInfo>> getDeltaFiles(SnapshotInfo fromSnapshot, SnapshotInfo toSnapshot,
      Set<String> tablesToLookup) throws IOException;
}

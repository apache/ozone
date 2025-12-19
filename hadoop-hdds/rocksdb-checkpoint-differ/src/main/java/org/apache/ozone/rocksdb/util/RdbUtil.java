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

package org.apache.ozone.rocksdb.util;

import static org.apache.hadoop.hdds.utils.IOUtils.getINode;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.rocksdb.LiveFileMetaData;

/**
 * Temporary class to test snapshot diff functionality.
 * This should be removed later.
 */
public final class RdbUtil {

  private RdbUtil() { }

  public static List<LiveFileMetaData> getLiveSSTFilesForCFs(final ManagedRocksDB rocksDB, Set<String> cfs) {
    return rocksDB.get().getLiveFilesMetaData().stream()
        .filter(lfm -> cfs.contains(StringUtils.bytes2String(lfm.columnFamilyName())))
        .collect(Collectors.toList());
  }

  public static Set<SstFileInfo> getSSTFilesForComparison(final ManagedRocksDB rocksDB, Set<String> cfs) {
    return getLiveSSTFilesForCFs(rocksDB, cfs).stream().map(SstFileInfo::new)
        .collect(Collectors.toCollection(HashSet::new));
  }

  public static Map<Object, SstFileInfo> getSSTFilesWithInodesForComparison(
      final ManagedRocksDB rocksDB, Set<String> cfs) throws IOException {
    List<LiveFileMetaData> liveSSTFilesForCFs = getLiveSSTFilesForCFs(rocksDB, cfs);
    Map<Object, SstFileInfo> inodeToSstMap = new HashMap<>();
    for (LiveFileMetaData lfm : liveSSTFilesForCFs) {
      Path sstFilePath = Paths.get(lfm.path(), lfm.fileName());
      Object inode = getINode(sstFilePath);
      inodeToSstMap.put(inode, new SstFileInfo(lfm));
    }
    return inodeToSstMap;
  }
}

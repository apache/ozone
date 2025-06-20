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

import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
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

  public static List<LiveFileMetaData> getLiveSSTFilesForCFs(
      final ManagedRocksDB rocksDB, List<String> cfs) {
    final Set<String> cfSet = Sets.newHashSet(cfs);
    return rocksDB.get().getLiveFilesMetaData().stream()
        .filter(lfm -> cfSet.contains(StringUtils.bytes2String(lfm.columnFamilyName())))
        .collect(Collectors.toList());
  }

  public static Set<String> getSSTFilesForComparison(
      final ManagedRocksDB rocksDB, List<String> cfs) {
    return getLiveSSTFilesForCFs(rocksDB, cfs).stream()
        .map(lfm -> new File(lfm.path(), lfm.fileName()).getPath())
        .collect(Collectors.toCollection(HashSet::new));
  }

  public static Map<Object, String> getSSTFilesWithInodesForComparison(final ManagedRocksDB rocksDB, List<String> cfs)
      throws IOException {
    List<LiveFileMetaData> liveSSTFilesForCFs = getLiveSSTFilesForCFs(rocksDB, cfs);
    Map<Object, String> inodeToSstMap = new HashMap<>();
    for (LiveFileMetaData lfm : liveSSTFilesForCFs) {
      Path sstFilePath = Paths.get(lfm.path(), lfm.fileName());
      Object inode = Files.readAttributes(sstFilePath, BasicFileAttributes.class).fileKey();
      inodeToSstMap.put(inode, sstFilePath.toString());
    }
    return inodeToSstMap;
  }
}

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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReaderIterator;
import org.rocksdb.SstFileReader;
import org.rocksdb.TableProperties;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Helper methods for snap-diff operations.
 */
public final class RocksDiffUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDiffUtils.class);

  private RocksDiffUtils() {
  }

  public static boolean isKeyWithPrefixPresent(String prefixForColumnFamily,
                                               String firstDbKey,
                                               String lastDbKey) {
    String firstKeyPrefix = constructBucketKey(firstDbKey);
    String endKeyPrefix = constructBucketKey(lastDbKey);
    return firstKeyPrefix.compareTo(prefixForColumnFamily) <= 0
        && prefixForColumnFamily.compareTo(endKeyPrefix) <= 0;
  }

  public static String constructBucketKey(String keyName) {
    if (!keyName.startsWith(OM_KEY_PREFIX)) {
      keyName = OM_KEY_PREFIX.concat(keyName);
    }
    String[] elements = keyName.split(OM_KEY_PREFIX);
    String volume = elements[1];
    String bucket = elements[2];
    StringBuilder builder =
        new StringBuilder().append(OM_KEY_PREFIX).append(volume);

    if (StringUtils.isNotBlank(bucket)) {
      builder.append(OM_KEY_PREFIX).append(bucket);
    }
    builder.append(OM_KEY_PREFIX);
    return builder.toString();
  }

  public static void filterRelevantSstFiles(Set<String> inputFiles,
      Map<String, String> tableToPrefixMap) throws IOException {
    for (Iterator<String> fileIterator =
         inputFiles.iterator(); fileIterator.hasNext();) {
      String filepath = fileIterator.next();
      if (!RocksDiffUtils.doesSstFileContainKeyRange(filepath,
          tableToPrefixMap)) {
        fileIterator.remove();
      }
    }
  }

  public static boolean doesSstFileContainKeyRange(String filepath,
      Map<String, String> tableToPrefixMap) throws IOException {

    try (
        ManagedOptions options = new ManagedOptions();
        ManagedSstFileReader sstFileReader = ManagedSstFileReader.managed(new SstFileReader(options))) {
      sstFileReader.get().open(filepath);
      TableProperties properties = sstFileReader.get().getTableProperties();
      String tableName = new String(properties.getColumnFamilyName(), UTF_8);
      if (tableToPrefixMap.containsKey(tableName)) {
        String prefix = tableToPrefixMap.get(tableName);

        try (
            ManagedReadOptions readOptions = new ManagedReadOptions();
            ManagedSstFileReaderIterator iterator = ManagedSstFileReaderIterator.managed(
                sstFileReader.get().newIterator(readOptions))) {
          iterator.get().seek(prefix.getBytes(UTF_8));
          String seekResultKey = new String(iterator.get().key(), UTF_8);
          return seekResultKey.startsWith(prefix);
        }
      }
      return false;
    } catch (RocksDBException e) {
      LOG.error("Failed to read SST File ", e);
      throw new IOException(e);
    }
  }


}

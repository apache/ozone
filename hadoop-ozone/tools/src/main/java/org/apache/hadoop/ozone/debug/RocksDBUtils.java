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

package org.apache.hadoop.ozone.debug;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * RocksDB specific utility functions.
 */
public final class RocksDBUtils {

  /** Never constructed. **/
  private RocksDBUtils() {
  }

  public static List<ColumnFamilyDescriptor> getColumnFamilyDescriptors(
      String dbPath) throws RocksDBException {
    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    List<byte[]> cfList = RocksDatabase.listColumnFamiliesEmptyOptions(dbPath);
    if (cfList != null) {
      for (byte[] b : cfList) {
        cfs.add(new ColumnFamilyDescriptor(b));
      }
    }
    return cfs;
  }

  public static ColumnFamilyHandle getColumnFamilyHandle(String columnFamilyName, List<ColumnFamilyHandle> cfHandleList)
      throws RocksDBException {
    byte[] nameBytes = columnFamilyName.getBytes(StandardCharsets.UTF_8);

    for (ColumnFamilyHandle cf : cfHandleList) {
      if (Arrays.equals(cf.getName(), nameBytes)) {
        return cf;
      }
    }

    return null;
  }

  public static <T> T getValue(ManagedRocksDB db,
                               ColumnFamilyHandle columnFamilyHandle, String key,
                               Codec<T> codec)
      throws IOException, RocksDBException {
    byte[] bytes = db.get().get(columnFamilyHandle,
        StringCodec.get().toPersistedFormat(key));
    return bytes != null ? codec.fromPersistedFormat(bytes) : null;
  }
}

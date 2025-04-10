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

package org.apache.ozone.util;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to benchmark splitIterator.
 */
public final class BenchmarkOmDBSplitItr {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkOmDBSplitItr.class);

  private BenchmarkOmDBSplitItr() {
  }

  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setInt(OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES, -1);
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS, args[0]);
    RocksDBConfiguration rocksDBConfiguration = ozoneConfiguration.getObject(RocksDBConfiguration.class);
    int maxThreads = Integer.parseInt(args[1]);
    long loggingThreshold = Long.parseLong(args[2]);
    rocksDBConfiguration.setParallelIteratorMaxPoolSize(maxThreads);
    ozoneConfiguration.setFromObject(rocksDBConfiguration);
    OmMetadataManagerImpl omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, null);
    AtomicLong counter = new AtomicLong();
    long startTime = System.currentTimeMillis();
    CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Void, IOException> func = kv -> {
      long id = counter.incrementAndGet();
      if (id % loggingThreshold == 0) {
        LOG.info("Benchmark: " + id + "\t" + (System.currentTimeMillis() - startTime) + "\t" + kv.getKey());
      }
      return null;
    };
    omMetadataManager.getFileTable().splitTableOperation(null, null, func,
        LOG, 1);
    omMetadataManager.getStore().close();
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import static org.apache.hadoop.hdds.conf.ConfigTag.OM;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

/**
 * Holds configuration items for OM RocksDB.
 */
@ConfigGroup(prefix = "hadoop.hdds.db")
public class RocksDBConfiguration {

  @Config(key = "rocksdb.logging.enabled",
      type = ConfigType.BOOLEAN,
      defaultValue = "false",
      tags = {OM, SCM, DATANODE},
      description = "Enable/Disable RocksDB logging for OM.")
  private boolean rocksdbLogEnabled;

  @Config(key = "rocksdb.logging.level",
      type = ConfigType.STRING,
      defaultValue = "INFO",
      tags = {OM, SCM, DATANODE},
      description = "OM RocksDB logging level (INFO/DEBUG/WARN/ERROR/FATAL)")
  private String rocksdbLogLevel;

  @Config(key = "rocksdb.writeoption.sync",
      type = ConfigType.BOOLEAN,
      defaultValue = "false",
      tags = {OM, SCM, DATANODE},
      description = "Enable/Disable Sync option. If true write will be " +
          "considered complete, once flushed to persistent storage. If false," +
          " writes are flushed asynchronously.")
  private boolean syncOption;

  @Config(key = "rocksdb.WAL_ttl_seconds",
      type = ConfigType.LONG,
      defaultValue = "1200",
      tags = {OM, SCM, DATANODE},
      description = "The lifetime of WAL log files. Default 1200 seconds.")
  private long walTTL = 1200;

  @Config(key = "rocksdb.WAL_size_limit_MB",
      type = ConfigType.SIZE,
      defaultValue = "0MB",
      tags = {OM, SCM, DATANODE},
      description = "The total size limit of WAL log files. Once the total log"
          + " file size exceeds this limit, the earliest files will be deleted."
          + "Default 0 means no limit.")
  private long walSizeLimit = 0;

  public void setRocksdbLoggingEnabled(boolean enabled) {
    this.rocksdbLogEnabled = enabled;
  }

  public boolean isRocksdbLoggingEnabled() {
    return rocksdbLogEnabled;
  }

  public void setRocksdbLogLevel(String level) {
    this.rocksdbLogLevel = level;
  }

  public String getRocksdbLogLevel() {
    return rocksdbLogLevel;
  }

  public void setSyncOption(boolean enabled) {
    this.syncOption = enabled;
  }

  public boolean getSyncOption() {
    return syncOption;
  }

  public void setWalTTL(long ttl) {
    this.walTTL = ttl;
  }

  public long getWalTTL() {
    return walTTL;
  }

  public void setWalSizeLimit(long limit) {
    this.walSizeLimit = limit;
  }

  public long getWalSizeLimit() {
    return walSizeLimit;
  }
}

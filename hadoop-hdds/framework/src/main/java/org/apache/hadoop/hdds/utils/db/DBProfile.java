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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.utils.db;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.math.BigDecimal;

/**
 * User visible configs based RocksDB tuning page. Documentation for Options.
 * <p>
 * https://github.com/facebook/rocksdb/blob/master/include/rocksdb/options.h
 * <p>
 * Most tuning parameters are based on this URL.
 * <p>
 * https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
 */
public enum DBProfile {
  //TODO : Add more profiles like TEST etc.
  SSD {
    @Override
    public String toString() {
      return "SSD";
    }

    @Override
    public ColumnFamilyOptions getColumnFamilyOptions() {
      return new ColumnFamilyOptions();
    }

    @Override
    public DBOptions getDBOptions() {
      return new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
    }


  },
  DISK {
    @Override
    public String toString() {
      return "DISK";
    }

    @Override
    public DBOptions getDBOptions() {
      return SSD.getDBOptions();
    }

    @Override
    public ColumnFamilyOptions getColumnFamilyOptions() {
      ColumnFamilyOptions columnFamilyOptions = SSD.getColumnFamilyOptions();
      return columnFamilyOptions;
    }


  };

  private static long toLong(double value) {
    BigDecimal temp = BigDecimal.valueOf(value);
    return temp.longValue();
  }

  public abstract DBOptions getDBOptions();

  public abstract ColumnFamilyOptions getColumnFamilyOptions();
}

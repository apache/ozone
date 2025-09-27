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

package org.apache.hadoop.ozone.recon.tasks;

/**
 * Value class for global statistics stored in RocksDB.
 * Contains the statistic value and last updated timestamp (as epoch milliseconds)
 * for efficient storage in the GLOBAL_STATS column family.
 */
public class GlobalStatsValue {
  private final Long value;
  private final Long lastUpdatedTimestamp; // epoch milliseconds for RocksDB storage

  public GlobalStatsValue(Long value, Long lastUpdatedTimestamp) {
    this.value = value;
    this.lastUpdatedTimestamp = lastUpdatedTimestamp;
  }

  public Long getValue() {
    return value;
  }

  public Long getLastUpdatedTimestamp() {
    return lastUpdatedTimestamp;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GlobalStatsValue) {
      GlobalStatsValue other = (GlobalStatsValue) obj;
      return (value != null ? value.equals(other.value) : other.value == null) &&
          (lastUpdatedTimestamp != null ? lastUpdatedTimestamp.equals(other.lastUpdatedTimestamp) : 
              other.lastUpdatedTimestamp == null);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = value != null ? value.hashCode() : 0;
    result = 31 * result + (lastUpdatedTimestamp != null ? lastUpdatedTimestamp.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "GlobalStatsValue{" +
        "value=" + value +
        ", lastUpdatedTimestamp=" + lastUpdatedTimestamp +
        '}';
  }
}

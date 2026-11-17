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

package org.apache.hadoop.hdds.utils.db;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulates a store's prefix info corresponding to tables in a db.
 */
public class TablePrefixInfo {
  private final Map<String, String> tablePrefixes;

  public TablePrefixInfo(Map<String, String> tablePrefixes) {
    this.tablePrefixes = Collections.unmodifiableMap(tablePrefixes);
  }

  public String getTablePrefix(String tableName) {
    return tablePrefixes.getOrDefault(tableName, "");
  }

  public int size() {
    return tablePrefixes.size();
  }

  public Set<String> getTableNames() {
    return tablePrefixes.keySet();
  }

  @Override
  public String toString() {
    return "TablePrefixInfo{" +
        "tablePrefixes=" + tablePrefixes +
        '}';
  }
}

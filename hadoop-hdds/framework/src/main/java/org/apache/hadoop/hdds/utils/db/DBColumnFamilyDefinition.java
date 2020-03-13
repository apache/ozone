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

import java.io.IOException;

/**
 * Class represents one single column table with the required codecs and types.
 *
 * @param <KEY>   the type of the key.
 * @param <VALUE> they type of the value.
 */
public class DBColumnFamilyDefinition<KEY, VALUE> {

  private String tableName;

  private Class<KEY> keyType;

  private Codec<KEY> keyCodec;

  private Class<VALUE> valueType;

  private Codec<VALUE> valueCodec;

  public DBColumnFamilyDefinition(
      String tableName,
      Class<KEY> keyType,
      Codec<KEY> keyCodec,
      Class<VALUE> valueType,
      Codec<VALUE> valueCodec) {
    this.tableName = tableName;
    this.keyType = keyType;
    this.keyCodec = keyCodec;
    this.valueType = valueType;
    this.valueCodec = valueCodec;
  }

  public Table<KEY, VALUE> getTable(DBStore db) throws IOException {
    return db.getTable(tableName, keyType, valueType);
  }

  public void registerTable(DBStoreBuilder storeBuilder) {
    storeBuilder.addTable(tableName)
        .addCodec(keyType, keyCodec)
        .addCodec(valueType, valueCodec);
  }

  public String getName() {
    return tableName;
  }
}

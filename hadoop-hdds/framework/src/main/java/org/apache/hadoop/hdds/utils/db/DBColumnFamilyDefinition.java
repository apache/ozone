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

import static org.apache.ratis.util.JavaUtils.getClassSimpleName;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.CollectionUtils;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;

/**
 * Class represents one single column table with the required codecs and types.
 *
 * @param <KEY>   the type of the key.
 * @param <VALUE> they type of the value.
 */
public class DBColumnFamilyDefinition<KEY, VALUE> {

  private final String tableName;
  private final Codec<KEY> keyCodec;
  private final Codec<VALUE> valueCodec;
  private final String name;

  private volatile ManagedColumnFamilyOptions cfOptions;

  public DBColumnFamilyDefinition(String tableName, Codec<KEY> keyCodec, Codec<VALUE> valueCodec) {
    this.tableName = Objects.requireNonNull(tableName, "tableName == null");
    this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec == null");
    this.valueCodec = Objects.requireNonNull(valueCodec, "valueCodec == null");
    this.name = tableName + "-def: " + getClassSimpleName(getKeyType())
        + " -> " + getClassSimpleName(getValueType());
    this.cfOptions = null;
  }

  public static Map<String, DBColumnFamilyDefinition<?, ?>> newUnmodifiableMap(
      DBColumnFamilyDefinition<?, ?>... families) {
    return newUnmodifiableMap(Collections.emptyMap(), families);
  }

  public static Map<String, DBColumnFamilyDefinition<?, ?>> newUnmodifiableMap(
      Map<String, DBColumnFamilyDefinition<?, ?>> existing,
      DBColumnFamilyDefinition<?, ?>... families) {
    return CollectionUtils.newUnmodifiableMap(Arrays.asList(families),
        DBColumnFamilyDefinition::getName, existing);
  }

  public static Map<String, List<DBColumnFamilyDefinition<?, ?>>> newUnmodifiableMultiMap(
      DBColumnFamilyDefinition<?, ?>... families
  ) {
    return CollectionUtils.newUnmodifiableMultiMap(Arrays.asList(families),
        DBColumnFamilyDefinition::getName);
  }

  public Table<KEY, VALUE> getTable(DBStore db) throws RocksDatabaseException, CodecException {
    return db.getTable(tableName, keyCodec, valueCodec);
  }

  public Table<KEY, VALUE> getTable(DBStore db, CacheType cacheType)
      throws RocksDatabaseException, CodecException {
    return db.getTable(tableName, keyCodec, valueCodec, cacheType);
  }

  public String getName() {
    return tableName;
  }

  public Class<KEY> getKeyType() {
    return keyCodec.getTypeClass();
  }

  public Codec<KEY> getKeyCodec() {
    return keyCodec;
  }

  public Class<VALUE> getValueType() {
    return valueCodec.getTypeClass();
  }

  public Codec<VALUE> getValueCodec() {
    return valueCodec;
  }

  public ManagedColumnFamilyOptions getCfOptions() {
    return this.cfOptions;
  }

  public void setCfOptions(ManagedColumnFamilyOptions cfOptions) {
    this.cfOptions = cfOptions;
  }

  @Override
  public String toString() {
    return name;
  }
}

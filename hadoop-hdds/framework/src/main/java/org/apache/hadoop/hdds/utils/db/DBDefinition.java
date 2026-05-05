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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple interface to provide information to create a DBStore..
 */
public interface DBDefinition {

  Logger LOG = LoggerFactory.getLogger(DBDefinition.class);

  /**
   * Logical name of the DB.
   */
  String getName();

  /**
   * Configuration key defines the location of the DB.
   */
  String getLocationConfigKey();

  /**
   * @param conf The configuration for the DB.
   * @return The parent directory for this definition's .db file.
   */
  default File getDBLocation(ConfigurationSource conf) {
    return ServerUtils.getDirectoryFromConfig(conf,
        getLocationConfigKey(), getName());
  }

  default Path getOptionsPath(ConfigurationSource conf) {
    return Paths.get("");
  }

  static List<String> getColumnFamilyNames(Iterable<DBColumnFamilyDefinition<?, ?>> columnFamilies) {
    return Collections.unmodifiableList(StreamSupport.stream(columnFamilies.spliterator(), false)
        .map(DBColumnFamilyDefinition::getName)
        .collect(Collectors.toList()));
  }

  default List<String> getColumnFamilyNames() {
    return getColumnFamilyNames(getColumnFamilies());
  }

  /**
   * @return The column families present in the DB.
   */
  Iterable<DBColumnFamilyDefinition<?, ?>> getColumnFamilies();

  /**
   * @return The column families for the given name.
   */
  List<DBColumnFamilyDefinition<?, ?>> getColumnFamilies(String name);

  /**
   * @return The unique column family for the given name.
   */
  default DBColumnFamilyDefinition<?, ?> getColumnFamily(String name) {
    final List<DBColumnFamilyDefinition<?, ?>> list = getColumnFamilies(name);
    if (list == null || list.isEmpty()) {
      return null;
    }
    if (list.size() > 1) {
      throw new IllegalStateException("Multi-valued: The name " + name
          + " maps to multiple values " + list + " in " + getName());
    }
    return list.get(0);
  }

  /**
   * Define a {@link WithMapInterface#getMap()} method
   * to implement {@link #getColumnFamily(String)}
   * and {@link #getColumnFamilies()}.
   */
  interface WithMapInterface extends DBDefinition {
    /** @return the underlying map. */
    Map<String, DBColumnFamilyDefinition<?, ?>> getMap();

    @Override
    default Collection<DBColumnFamilyDefinition<?, ?>> getColumnFamilies() {
      return getMap().values();
    }

    @Override
    default List<DBColumnFamilyDefinition<?, ?>> getColumnFamilies(
        String name) {
      final DBColumnFamilyDefinition<?, ?> d = getMap().get(name);
      return d != null ? Collections.singletonList(d) : Collections.emptyList();
    }
  }

  /**
   * Provide constructors to initialize {@link #map}
   * and use it to implement {@link #getMap()}.
   */
  abstract class WithMap implements WithMapInterface {
    private final Map<String, DBColumnFamilyDefinition<?, ?>> map;
    private final Supplier<List<String>> columnFamilyNames;

    protected WithMap(Map<String, DBColumnFamilyDefinition<?, ?>> map) {
      this.map = map;
      this.columnFamilyNames = MemoizedSupplier.valueOf(
          () -> DBDefinition.getColumnFamilyNames(getColumnFamilies()));
    }

    @Override
    public final List<String> getColumnFamilyNames() {
      return columnFamilyNames.get();
    }

    @Override
    public final Map<String, DBColumnFamilyDefinition<?, ?>> getMap() {
      return map;
    }
  }
}

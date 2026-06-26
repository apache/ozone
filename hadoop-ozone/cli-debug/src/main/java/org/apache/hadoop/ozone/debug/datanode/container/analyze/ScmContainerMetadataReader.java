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

package org.apache.hadoop.ozone.debug.datanode.container.analyze;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Read-only lookup of container metadata from {@code scm.db}.
 */
public final class ScmContainerMetadataReader implements AutoCloseable {

  private final DBStore dbStore;
  private final Table<ContainerID, ContainerInfo> containerTable;

  public ScmContainerMetadataReader(ConfigurationSource conf, File scmDbPath)
      throws IOException {
    File scmDbDir = resolveScmDbDirectory(scmDbPath);
    File parentDir = scmDbDir.getParentFile();
    if (parentDir == null) {
      throw new IOException("SCM database directory has no parent path: " + scmDbDir);
    }
    try {
      this.dbStore = DBStoreBuilder.newBuilder(conf, SCMDBDefinition.get(), scmDbDir.getName(),
          parentDir.toPath())
          .setOpenReadOnly(true)
          .build();
    } catch (RocksDatabaseException e) {
      throw new IOException("Failed to open SCM database at " + scmDbDir, e);
    }
    try {
      this.containerTable = CONTAINERS.getTable(dbStore, CacheType.NO_CACHE);
    } catch (RocksDatabaseException | CodecException e) {
      dbStore.close();
      throw new IOException("Failed to open scm.db containers column family at " + scmDbDir, e);
    }
  }

  /**
   * Classify a container ID against scm.db {@code containers}.
   *
   * @return {@link Optional#empty()} when the container is present in SCM with a
   *     non-DELETED lifecycle state
   */
  public Optional<ScmContainerClassification> classify(long containerId) throws IOException {
    try {
      ContainerInfo info = containerTable.get(ContainerID.valueOf(containerId));
      if (info == null) {
        return Optional.of(ScmContainerClassification.NOT_IN_SCM);
      }
      if (info.isDeleted()) {
        return Optional.of(ScmContainerClassification.DELETED);
      }
      return Optional.empty();
    } catch (RocksDatabaseException | CodecException e) {
      throw new IOException("Failed to read container " + containerId + " from scm.db", e);
    }
  }

  static File resolveScmDbDirectory(File path) throws IOException {
    Objects.requireNonNull(path, "scmDbPath");
    File absolutePath = path.getAbsoluteFile();
    File scmDbDir = absolutePath;
    if (!OzoneConsts.SCM_DB_NAME.equals(absolutePath.getName())) {
      File child = new File(absolutePath, OzoneConsts.SCM_DB_NAME);
      if (child.isDirectory()) {
        scmDbDir = child;
      }
    }
    if (!scmDbDir.isDirectory()) {
      throw new IOException("SCM database directory not found: " + path);
    }
    return scmDbDir;
  }

  @Override
  public void close() {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  /**
   * SCM-side classification for an on-disk container directory.
   */
  enum ScmContainerClassification {
    /** No record for this container ID in scm.db {@code containers}. */
    NOT_IN_SCM,
    /** Record exists and {@link ContainerInfo} state is DELETED. */
    DELETED
  }
}

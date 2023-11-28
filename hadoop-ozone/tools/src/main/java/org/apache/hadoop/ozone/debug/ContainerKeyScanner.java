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

package org.apache.hadoop.ozone.debug;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Parser for a list of container IDs, to scan for keys.
 */
@CommandLine.Command(
    name = "ckscanner",
    description = "Parse a list of container IDs"
)
@MetaInfServices(SubcommandWithParent.class)
// TODO use dirinfotable field as FSOdirpathresolver
public class ContainerKeyScanner implements Callable<Void>,
    SubcommandWithParent {

  public static final String FILE_TABLE = "fileTable";
  public static final String KEY_TABLE = "keyTable";
  public static final String DIRECTORY_TABLE = "directoryTable";

  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;

  @CommandLine.ParentCommand
  private RDBParser parent;

  @CommandLine.Option(names = {"-ids", "--container-ids"},
      split = ",",
      paramLabel = "containerIDs",
      required = true,
      description = "Set of container IDs to be used for getting all " +
          "their keys. Example-usage: 1,11,2.(Separated by ',')")
  private Set<Long> containerIds;

  @Override
  public Void call() throws Exception {
    ContainerKeyInfoWrapper containerKeyInfoWrapper =
        scanDBForContainerKeys(parent.getDbPath());

    printOutput(containerKeyInfoWrapper);

    return null;
  }

  @Override
  public Class<?> getParentType() {
    return RDBParser.class;
  }

  private static PrintWriter err() {
    return spec.commandLine().getErr();
  }

  private static PrintWriter out() {
    return spec.commandLine().getOut();
  }

  // TODO optimize this method to use single objectId instead of a set
  //  and to return pair of objectId and path instead of a map.
  //  Further optimization could be done to reuse db
  //  and not connect to it for every method call
  @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
  public Map<Long, Path> getAbsolutePathForObjectIDs(
      long bucketId, String prefix, Optional<Set<Long>> dirObjIds,
      String dbPath)
      throws IOException, RocksDBException {
    // Root of a bucket would always have the
    // key as /volumeId/bucketId/bucketId/
    if (!dirObjIds.isPresent() || dirObjIds.get().isEmpty()) {
      return Collections.emptyMap();
    }
    Set<Long> objIds = Sets.newHashSet(dirObjIds.get());
    Map<Long, Path> objectIdPathMap = new HashMap<>();
    Queue<Pair<Long, Path>> objectIdPathVals = new LinkedList<>();
    Pair<Long, Path> root = Pair.of(bucketId, Paths.get(OZONE_URI_DELIMITER));
    objectIdPathVals.add(root);
    addToPathMap(root, objIds, objectIdPathMap);

    while (!objectIdPathVals.isEmpty() && !objIds.isEmpty()) {
      Pair<Long, Path> parentPair = objectIdPathVals.poll();

      // Get all tables from RocksDB
      List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          RocksDBUtils.getColumnFamilyDescriptors(dbPath);
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      // Get all table handles
      try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(dbPath,
          columnFamilyDescriptors, columnFamilyHandles)) {
        dbPath = removeTrailingSlashIfNeeded(dbPath);
        DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(
            Paths.get(dbPath), new OzoneConfiguration());
        if (dbDefinition == null) {
          throw new IllegalStateException("Incorrect DB Path");
        }

        // Get directory table
        DBColumnFamilyDefinition<?, ?> columnFamilyDefinition =
            dbDefinition.getColumnFamily(DIRECTORY_TABLE);
        if (columnFamilyDefinition == null) {
          throw new IllegalStateException(
              "Table with name" + DIRECTORY_TABLE + " not found");
        }

        // Get directory table handle
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
            columnFamilyDefinition.getName().getBytes(UTF_8),
            columnFamilyHandles);
        if (columnFamilyHandle == null) {
          throw new IllegalStateException("columnFamilyHandle is null");
        }

        // Get iterator for directory table
        try (ManagedRocksIterator iterator = new ManagedRocksIterator(
            db.get().newIterator(columnFamilyHandle))) {
          iterator.get().seekToFirst();
          while (!objIds.isEmpty() && iterator.get().isValid()) {
            String subDir = prefix + parentPair.getKey() + OM_KEY_PREFIX;
            String key = new String(iterator.get().key(), UTF_8);

            // Skip key if it does not contain subDir
            if (!key.contains(subDir)) {
              iterator.get().next();
              continue;
            }

            OmDirectoryInfo childDir =
                ((OmDirectoryInfo) columnFamilyDefinition.getValueCodec()
                    .fromPersistedFormat(iterator.get().value()));
            Pair<Long, Path> pathVal = Pair.of(childDir.getObjectID(),
                parentPair.getValue().resolve(childDir.getName()));
            addToPathMap(pathVal, objIds, objectIdPathMap);
            objectIdPathVals.add(pathVal);
            iterator.get().next();
          }
        }
      }
    }
    // Invalid directory objectId which does not exist in the given bucket.
    if (!objIds.isEmpty()) {
      throw new IllegalArgumentException(
          "Dir object Ids required but not found in bucket: " + objIds);
    }
    return objectIdPathMap;
  }

  private void addToPathMap(Pair<Long, Path> objectIDPath,
                            Set<Long> dirObjIds, Map<Long, Path> pathMap) {
    if (dirObjIds.contains(objectIDPath.getKey())) {
      pathMap.put(objectIDPath.getKey(), objectIDPath.getValue());
      dirObjIds.remove(objectIDPath.getKey());
    }
  }

  private ContainerKeyInfoWrapper scanDBForContainerKeys(String dbPath)
      throws RocksDBException, IOException {
    List<ContainerKeyInfo> containerKeyInfos = new ArrayList<>();

    List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        RocksDBUtils.getColumnFamilyDescriptors(dbPath);
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    long keysProcessed = 0;

    try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(dbPath,
        columnFamilyDescriptors, columnFamilyHandles)) {
      dbPath = removeTrailingSlashIfNeeded(dbPath);
      DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(
          Paths.get(dbPath), new OzoneConfiguration());
      if (dbDefinition == null) {
        throw new IllegalStateException("Incorrect DB Path");
      }

      keysProcessed +=
          processTable(dbDefinition, columnFamilyHandles, db,
              containerKeyInfos, FILE_TABLE, dbPath);
      keysProcessed +=
          processTable(dbDefinition, columnFamilyHandles, db,
              containerKeyInfos, KEY_TABLE, dbPath);
    }
    return new ContainerKeyInfoWrapper(keysProcessed, containerKeyInfos);
  }

  private long processTable(DBDefinition dbDefinition,
                            List<ColumnFamilyHandle> columnFamilyHandles,
                            ManagedRocksDB db,
                            List<ContainerKeyInfo> containerKeyInfos,
                            String tableName, String dbPath)
      throws IOException, RocksDBException {
    long keysProcessed = 0;
    DBColumnFamilyDefinition<?, ?> columnFamilyDefinition =
        dbDefinition.getColumnFamily(tableName);
    if (columnFamilyDefinition == null) {
      throw new IllegalStateException(
          "Table with name" + tableName + " not found");
    }

    ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
        columnFamilyDefinition.getName().getBytes(UTF_8),
        columnFamilyHandles);
    if (columnFamilyHandle == null) {
      throw new IllegalStateException("columnFamilyHandle is null");
    }

    try (ManagedRocksIterator iterator = new ManagedRocksIterator(
        db.get().newIterator(columnFamilyHandle))) {
      iterator.get().seekToFirst();
      while (iterator.get().isValid()) {
        OmKeyInfo value = ((OmKeyInfo) columnFamilyDefinition.getValueCodec()
            .fromPersistedFormat(iterator.get().value()));
        List<OmKeyLocationInfoGroup> keyLocationVersions =
            value.getKeyLocationVersions();
        if (Objects.isNull(keyLocationVersions)) {
          iterator.get().next();
          keysProcessed++;
          continue;
        }

        long volumeId = 0;
        long bucketId = 0;
        // volumeId and bucketId are only applicable to file table
        if (tableName.equals(FILE_TABLE)) {
          String key = new String(iterator.get().key(), UTF_8);
          String[] keyParts = key.split(OM_KEY_PREFIX);
          volumeId = Long.parseLong(keyParts[1]);
          bucketId = Long.parseLong(keyParts[2]);
        }

        for (OmKeyLocationInfoGroup locationInfoGroup : keyLocationVersions) {
          for (List<OmKeyLocationInfo> locationInfos :
              locationInfoGroup.getLocationVersionMap().values()) {
            for (OmKeyLocationInfo locationInfo : locationInfos) {
              if (containerIds.contains(locationInfo.getContainerID())) {
                // Generate asbolute key path for FSO keys
                StringBuilder keyName = new StringBuilder();
                if (tableName.equals(FILE_TABLE)) {
                  keyName.append(
                      getFsoKeyPrefix(dbPath, volumeId, bucketId, value));
                }
                keyName.append(value.getKeyName());
                containerKeyInfos.add(
                    new ContainerKeyInfo(locationInfo.getContainerID(),
                        value.getVolumeName(), volumeId, value.getBucketName(),
                        bucketId, keyName.toString(),
                        value.getParentObjectID()));
              }
            }
          }
        }
        iterator.get().next();
        keysProcessed++;
      }
      return keysProcessed;
    }
  }

  private static String removeBeginningSlash(String path) {
    if (path.startsWith(OM_KEY_PREFIX)) {
      return path.substring(1);
    }

    return path;
  }

  private String getFsoKeyPrefix(String dbPath, long volumeId, long bucketId,
                                 OmKeyInfo value)
      throws IOException, RocksDBException {
    String prefix =
        OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId +
            OM_KEY_PREFIX;
    Set<Long> dirObjIds = new HashSet<>();
    dirObjIds.add(value.getParentObjectID());
    Map<Long, Path> absolutePaths =
        getAbsolutePathForObjectIDs(bucketId, prefix,
            Optional.of(dirObjIds), dbPath);
    Path path = absolutePaths.get(value.getParentObjectID());
    String keyPath;
    if (path.toString().equals(OM_KEY_PREFIX)) {
      keyPath = path.toString();
    } else {
      keyPath = path + OM_KEY_PREFIX;
    }

    return removeBeginningSlash(keyPath);
  }


  private ColumnFamilyHandle getColumnFamilyHandle(
      byte[] name, List<ColumnFamilyHandle> columnFamilyHandles) {
    return columnFamilyHandles
        .stream()
        .filter(
            handle -> {
              try {
                return Arrays.equals(handle.getName(), name);
              } catch (Exception ex) {
                throw new RuntimeException(ex);
              }
            })
        .findAny()
        .orElse(null);
  }

  private String removeTrailingSlashIfNeeded(String dbPath) {
    if (dbPath.endsWith(OzoneConsts.OZONE_URI_DELIMITER)) {
      dbPath = dbPath.substring(0, dbPath.length() - 1);
    }
    return dbPath;
  }

  private void printOutput(ContainerKeyInfoWrapper containerKeyInfoWrapper) {
    List<ContainerKeyInfo> containerKeyInfos =
        containerKeyInfoWrapper.getContainerKeyInfos();
    if (containerKeyInfos.isEmpty()) {
      try (PrintWriter out = out()) {
        out.println("No keys were found for container IDs: " +
            containerIds);
        out.println(
            "Keys processed: " + containerKeyInfoWrapper.getKeysProcessed());
      }
      return;
    }

    Map<Long, List<ContainerKeyInfo>> infoMap = new HashMap<>();

    for (long id : containerIds) {
      List<ContainerKeyInfo> tmpList = new ArrayList<>();

      for (ContainerKeyInfo info : containerKeyInfos) {
        if (id == info.getContainerID()) {
          tmpList.add(info);
        }
      }
      infoMap.put(id, tmpList);
    }

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String prettyJson = gson.toJson(
        new ContainerKeyInfoResponse(containerKeyInfoWrapper.getKeysProcessed(),
            infoMap));
    try (PrintWriter out = out()) {
      out.println(prettyJson);
    }
  }

}

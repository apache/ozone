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

package org.apache.hadoop.ozone.debug.om;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

/**
 * Tool to map full key paths that use the specified containers.
 * Note: Currently only processes FSO layout buckets.
 */
@CommandLine.Command(
    name = "container-key-mapping",
    aliases = "ckm",
    description = "Maps full key paths that use the specified containers. " +
        "Note: A container can have both FSO and OBS keys. Currently this tool processes only FSO keys")
public class ContainerToKeyMapping extends AbstractSubcommand implements Callable<Void> {
  private static final String DIRTREE_DB_NAME = "omdirtree.db";
  private static final String DIRTREE_TABLE_NAME = "dirTreeTable";
  private static final DBColumnFamilyDefinition<Long, String> DIRTREE_TABLE_DEFINITION =
      new DBColumnFamilyDefinition<>(DIRTREE_TABLE_NAME,
          LongCodec.get(),
          StringCodec.get());

  @CommandLine.ParentCommand
  private OMDebug parent;

  @CommandLine.Option(names = {"--containers"},
      required = true,
      description = "Comma separated Container IDs")
  private String containers;

  private ManagedRocksDB rocksDB;
  private ColumnFamilyHandle volumeCFHandle;
  private ColumnFamilyHandle bucketCFHandle;
  private ColumnFamilyHandle directoryCFHandle;
  private ColumnFamilyHandle fileCFHandle;
  private DBStore dirTreeDbStore;
  private Table<Long, String> dirTreeTable;
  // Cache volume IDs to avoid repeated lookups
  private final Map<String, Long> volumeCache = new HashMap<>();

  // TODO: Add support to OBS keys (HDDS-14118)
  @Override
  public Void call() throws Exception {
    err().println("Note: A container can have both FSO and OBS keys. Currently this tool processes only FSO keys");
    
    String dbPath = parent.getDbPath();
    // Parse container IDs
    Set<Long> containerIDs = Arrays.stream(containers.split(","))
        .map(String::trim)
        .map(Long::parseLong)
        .collect(Collectors.toSet());

    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(dbPath);
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();

    try (PrintWriter writer = out()) {
      rocksDB = ManagedRocksDB.openReadOnly(dbPath, cfDescList, cfHandleList);
      volumeCFHandle = RocksDBUtils.getColumnFamilyHandle(
          OMDBDefinition.VOLUME_TABLE_DEF.getName(), cfHandleList);
      bucketCFHandle = RocksDBUtils.getColumnFamilyHandle(
          OMDBDefinition.BUCKET_TABLE_DEF.getName(), cfHandleList);
      directoryCFHandle = RocksDBUtils.getColumnFamilyHandle(
          OMDBDefinition.DIRECTORY_TABLE_DEF.getName(), cfHandleList);
      fileCFHandle = RocksDBUtils.getColumnFamilyHandle(
          OMDBDefinition.FILE_TABLE_DEF.getName(), cfHandleList);

      openDirTreeDB(dbPath);
      retrieve(writer, containerIDs);
    } catch (RocksDBException e) {
      err().println("Failed to open RocksDB: " + e);
      throw e;
    } finally {
      closeDirTreeDB(dbPath);
      if (rocksDB != null) {
        rocksDB.close();
      }
    }
    return null;
  }

  private void openDirTreeDB(String dbPath) throws IOException {
    File dirTreeDbPath = new File(new File(dbPath).getParentFile(), DIRTREE_DB_NAME);
    // Delete the DB from the last run if it exists.
    if (dirTreeDbPath.exists()) {
      FileUtils.deleteDirectory(dirTreeDbPath);
    }

    ConfigurationSource conf = new OzoneConfiguration();
    dirTreeDbStore = DBStoreBuilder.newBuilder(conf)
        .setName(DIRTREE_DB_NAME)
        .setPath(dirTreeDbPath.getParentFile().toPath())
        .addTable(DIRTREE_TABLE_DEFINITION.getName())
        .build();
    dirTreeTable = dirTreeDbStore.getTable(DIRTREE_TABLE_DEFINITION.getName(),
        DIRTREE_TABLE_DEFINITION.getKeyCodec(), DIRTREE_TABLE_DEFINITION.getValueCodec());
  }

  private void closeDirTreeDB(String dbPath) throws IOException {
    if (dirTreeDbStore != null) {
      dirTreeDbStore.close();
    }
    File dirTreeDbPath = new File(new File(dbPath).getParentFile(), DIRTREE_DB_NAME);
    if (dirTreeDbPath.exists()) {
      FileUtils.deleteDirectory(dirTreeDbPath);
    }
  }

  private void retrieve(PrintWriter writer, Set<Long> containerIds) {
    // Build dir tree
    Map<Long, Pair<Long, String>> bucketVolMap = new HashMap<>();
    try {
      prepareDirIdTree(bucketVolMap);
    } catch (Exception e) {
      err().println("Exception occurred reading directory Table, " + e);
      return;
    }

    // Map to collect keys per container
    Map<Long, List<String>> containerToKeysMap = new HashMap<>();
    for (Long containerId : containerIds) {
      containerToKeysMap.put(containerId, new ArrayList<>());
    }

    // Iterate file table and filter for container
    try (ManagedRocksIterator fileIterator = new ManagedRocksIterator(
        rocksDB.get().newIterator(fileCFHandle))) {
      fileIterator.get().seekToFirst();
      
      while (fileIterator.get().isValid()) {
        byte[] value = fileIterator.get().value();
        OmKeyInfo keyInfo = OMDBDefinition.FILE_TABLE_DEF.getValueCodec().fromPersistedFormat(value);

        // Find which containers this key uses
        Set<Long> keyContainers = new HashSet<>();
        keyInfo.getKeyLocationVersions().forEach(
            e -> e.getLocationList().forEach(
                blk -> {
                  long cid = blk.getBlockID().getContainerID();
                  if (containerIds.contains(cid)) {
                    keyContainers.add(cid);
                  }
                }));

        if (!keyContainers.isEmpty()) {
          // Reconstruct full path
          String fullPath = reconstructFullPath(keyInfo, bucketVolMap);
          if (fullPath != null) {
            for (Long containerId : keyContainers) {
              containerToKeysMap.get(containerId).add(fullPath);
            }
          }
        }
        
        fileIterator.get().next();
      }
    } catch (Exception e) {
      err().println("Exception occurred reading file Table, " + e);
      return;
    }
    jsonOutput(writer, containerToKeysMap);
  }

  private void prepareDirIdTree(Map<Long, Pair<Long, String>> bucketVolMap) throws Exception {
    // Add bucket volume tree
    try (ManagedRocksIterator bucketIterator = new ManagedRocksIterator(
        rocksDB.get().newIterator(bucketCFHandle))) {
      bucketIterator.get().seekToFirst();
      
      while (bucketIterator.get().isValid()) {
        byte[] value = bucketIterator.get().value();
        OmBucketInfo bucketInfo = OMDBDefinition.BUCKET_TABLE_DEF.getValueCodec().fromPersistedFormat(value);
        String volumeName = bucketInfo.getVolumeName();

        // Get volume ID from volume table
        Long volumeId = getVolumeId(volumeName);
        if (volumeId == null) {
          bucketIterator.get().next();
          continue;
        }

        bucketVolMap.put(bucketInfo.getObjectID(), Pair.of(volumeId, bucketInfo.getBucketName()));
        bucketVolMap.putIfAbsent(volumeId, Pair.of(null, volumeName));
        
        bucketIterator.get().next();
      }
    }

    // Add dir tree
    try (ManagedRocksIterator directoryIterator = new ManagedRocksIterator(
        rocksDB.get().newIterator(directoryCFHandle))) {
      directoryIterator.get().seekToFirst();
      
      while (directoryIterator.get().isValid()) {
        byte[] value = directoryIterator.get().value();
        OmDirectoryInfo dirInfo = OMDBDefinition.DIRECTORY_TABLE_DEF.getValueCodec().fromPersistedFormat(value);
        addToDirTree(dirInfo.getObjectID(), dirInfo.getParentObjectID(), dirInfo.getName());
        
        directoryIterator.get().next();
      }
    }
  }

  private String reconstructFullPath(OmKeyInfo keyInfo, Map<Long, Pair<Long, String>> bucketVolMap) throws Exception {
    StringBuilder sb = new StringBuilder(keyInfo.getKeyName());
    Long prvParent = keyInfo.getParentObjectID();
    
    while (prvParent != null) {
      // Check reached for bucket volume level
      if (bucketVolMap.containsKey(prvParent)) {
        Pair<Long, String> nameParentPair = bucketVolMap.get(prvParent);
        sb.insert(0, nameParentPair.getValue() + OM_KEY_PREFIX);
        prvParent = nameParentPair.getKey();
        if (null == prvParent) {
          return sb.toString();
        }
        continue;
      }

      // Check dir tree
      Pair<Long, String> nameParentPair = getFromDirTree(prvParent);
      if (nameParentPair == null) {
        break;
      }
      sb.insert(0, nameParentPair.getValue() + OM_KEY_PREFIX);
      prvParent = nameParentPair.getKey();
    }
    return null;
  }

  private Pair<Long, String> getFromDirTree(Long objectId) throws IOException {
    String val = dirTreeTable.get(objectId);
    if (val == null) {
      return null;
    }
    return getDirParentNamePair(val);
  }

  public static Pair<Long, String> getDirParentNamePair(String val) {
    int hashIdx = val.lastIndexOf('#');
    String strParentId = val.substring(hashIdx + 1);
    Long parentId = null;
    if (!StringUtils.isEmpty(strParentId)) {
      parentId = Long.parseLong(strParentId);
    }
    return Pair.of(parentId, val.substring(0, hashIdx));
  }

  private Long getVolumeId(String volumeName) throws Exception {
    if (volumeCache.containsKey(volumeName)) {
      return volumeCache.get(volumeName);
    }
    
    String volumeKey = OM_KEY_PREFIX + volumeName;
    byte[] keyBytes = OMDBDefinition.VOLUME_TABLE_DEF.getKeyCodec().toPersistedFormat(volumeKey);
    byte[] valueBytes = rocksDB.get().get(volumeCFHandle, keyBytes);
    
    if (valueBytes != null) {
      OmVolumeArgs volumeArgs = OMDBDefinition.VOLUME_TABLE_DEF.getValueCodec().fromPersistedFormat(valueBytes);
      Long volumeId = volumeArgs.getObjectID();
      volumeCache.put(volumeName, volumeId);
      return volumeId;
    }
    return null;
  }

  private void addToDirTree(Long objectId, Long parentId, String name) throws IOException {
    if (null == parentId) {
      dirTreeTable.put(objectId, name + "#");
    } else {
      dirTreeTable.put(objectId, name + "#" + parentId);
    }
  }

  private void jsonOutput(PrintWriter writer, Map<Long, List<String>> containerToKeysMap) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode root = mapper.createObjectNode();
      ObjectNode containersNode = mapper.createObjectNode();

      for (Map.Entry<Long, List<String>> entry : containerToKeysMap.entrySet()) {
        ObjectNode containerNode = mapper.createObjectNode();
        ArrayNode keysArray = mapper.createArrayNode();
        
        for (String key : entry.getValue()) {
          keysArray.add(key);
        }
        
        containerNode.set("keys", keysArray);
        containerNode.put("numOfKeys", entry.getValue().size());
        containersNode.set(entry.getKey().toString(), containerNode);
      }

      root.set("containers", containersNode);
      writer.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
      writer.flush();
    } catch (Exception e) {
      err().println("Error writing JSON output: " + e.getMessage());
    }
  }
}


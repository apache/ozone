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
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import picocli.CommandLine;

/**
 * Tool to map full key paths that use the specified containers.
 * Supports both FSO (File System Optimized) and OBS (Object Store) bucket layouts.
 */
@CommandLine.Command(
    name = "container-key-mapping",
    aliases = "ckm",
    description = "Maps full key paths that use the specified containers.")
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

  @CommandLine.Option(names = {"--onlyFileNames"},
      defaultValue = "false",
      description = "Only display file names without full path")
  private boolean onlyFileNames;

  private DBStore omDbStore;
  private Table<String, OmVolumeArgs> volumeTable;
  private Table<String, OmBucketInfo> bucketTable;
  private Table<String, OmDirectoryInfo> directoryTable;
  private Table<String, OmKeyInfo> fileTable;
  private Table<String, OmKeyInfo> keyTable;
  private DBStore dirTreeDbStore;
  private Table<Long, String> dirTreeTable;
  // Cache volume IDs to avoid repeated lookups
  private final Map<String, Long> volumeCache = new HashMap<>();
  private ConfigurationSource conf;

  @Override
  public Void call() throws Exception {

    String dbPath = parent.getDbPath();
    // Parse container IDs
    Set<Long> containerIDs = Arrays.stream(containers.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(Long::parseLong)
        .collect(Collectors.toSet());

    if (containerIDs.isEmpty()) {
      err().println("No valid container IDs provided");
      return null;
    }

    conf = new OzoneConfiguration();
    File dbFile = new File(dbPath);

    try (PrintWriter writer = out()) {
      omDbStore = DBStoreBuilder.newBuilder(conf, OMDBDefinition.get(), dbFile.getName(),
          dbFile.getParentFile().toPath())
          .setOpenReadOnly(true)
          .build();

      volumeTable = OMDBDefinition.VOLUME_TABLE_DEF.getTable(omDbStore, CacheType.NO_CACHE);
      bucketTable = OMDBDefinition.BUCKET_TABLE_DEF.getTable(omDbStore, CacheType.NO_CACHE);
      directoryTable = OMDBDefinition.DIRECTORY_TABLE_DEF.getTable(omDbStore, CacheType.NO_CACHE);
      fileTable = OMDBDefinition.FILE_TABLE_DEF.getTable(omDbStore, CacheType.NO_CACHE);
      keyTable = OMDBDefinition.KEY_TABLE_DEF.getTable(omDbStore, CacheType.NO_CACHE);

      retrieve(dbPath, writer, containerIDs);
    } catch (Exception e) {
      err().println("Failed to open RocksDB: " + e);
      throw e;
    } finally {
      closeDirTreeDB(dbPath);
      if (omDbStore != null) {
        omDbStore.close();
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

  private void retrieve(String dbPath, PrintWriter writer, Set<Long> containerIds) {
    Map<Long, Pair<Long, String>> bucketVolMap = new HashMap<>();
    // Build dir tree for FSO keys only if we need full paths
    if (!onlyFileNames) {
      try {
        openDirTreeDB(dbPath);
        prepareDirIdTree(bucketVolMap);
      } catch (Exception e) {
        err().println("Exception occurred reading directory Table, " + e);
        return;
      }
    }

    // Map to collect keys per container
    Map<Long, List<String>> containerToKeysMap = new HashMap<>();
    // Track unreferenced keys count per container (FSO only)
    Map<Long, Long> unreferencedCountMap = new HashMap<>();
    for (Long containerId : containerIds) {
      containerToKeysMap.put(containerId, new ArrayList<>());
      unreferencedCountMap.put(containerId, 0L);
    }

    // Process FSO keys (fileTable)
    processFSOKeys(containerIds, containerToKeysMap, unreferencedCountMap, bucketVolMap);

    // Process OBS keys (keyTable)
    processOBSKeys(containerIds, containerToKeysMap);

    jsonOutput(writer, containerToKeysMap, unreferencedCountMap);
  }

  private void processFSOKeys(Set<Long> containerIds, Map<Long, List<String>> containerToKeysMap,
      Map<Long, Long> unreferencedCountMap, Map<Long, Pair<Long, String>> bucketVolMap) {
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> fileIterator =
        fileTable.iterator()) {
      
      while (fileIterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = fileIterator.next();
        OmKeyInfo keyInfo = entry.getValue();

        // Find which containers this key uses
        Set<Long> keyContainers = getKeyContainers(keyInfo, containerIds);

        if (!keyContainers.isEmpty()) {
          // For FSO keys, reconstruct the full path
          // Or extract just the key name if onlyFileNames is true
          String keyPath = onlyFileNames ? keyInfo.getKeyName() :
              reconstructFullPath(keyInfo, bucketVolMap, unreferencedCountMap, keyContainers);
          if (keyPath != null) {
            for (Long containerId : keyContainers) {
              containerToKeysMap.get(containerId).add(keyPath);
            }
          }
        }
      }
    } catch (Exception e) {
      err().println("Exception occurred reading fileTable (FSO keys), " + e);
    }
  }

  private void processOBSKeys(Set<Long> containerIds, Map<Long, List<String>> containerToKeysMap) {
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> keyIterator =
         keyTable.iterator()) {

      while (keyIterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = keyIterator.next();
        OmKeyInfo keyInfo = entry.getValue();

        // Find which containers this key uses
        Set<Long> keyContainers = getKeyContainers(keyInfo, containerIds);

        if (!keyContainers.isEmpty()) {
          // For OBS keys, use the database key directly (already in /volume/bucket/key format)
          // Or extract just the key name if onlyFileNames is true
          String keyPath = onlyFileNames ? keyInfo.getKeyName() : entry.getKey();
          for (Long containerId : keyContainers) {
            containerToKeysMap.get(containerId).add(keyPath);
          }
        }
      }
    } catch (Exception e) {
      err().println("Exception occurred reading keyTable (OBS keys), " + e);
    }
  }

  private Set<Long> getKeyContainers(OmKeyInfo keyInfo, Set<Long> targetContainerIds) {
    Set<Long> keyContainers = new HashSet<>();
    keyInfo.getKeyLocationVersions().forEach(
        e -> e.getLocationList().forEach(
            blk -> {
              long cid = blk.getBlockID().getContainerID();
              if (targetContainerIds.contains(cid)) {
                keyContainers.add(cid);
              }
            }));
    return keyContainers;
  }

  private void prepareDirIdTree(Map<Long, Pair<Long, String>> bucketVolMap) throws Exception {
    // Add bucket volume tree
    try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>> bucketIterator = 
        bucketTable.iterator()) {
      
      while (bucketIterator.hasNext()) {
        Table.KeyValue<String, OmBucketInfo> entry = bucketIterator.next();
        OmBucketInfo bucketInfo = entry.getValue();
        String volumeName = bucketInfo.getVolumeName();

        // Get volume ID from volume table
        Long volumeId = getVolumeId(volumeName);
        if (volumeId == null) {
          continue;
        }

        bucketVolMap.put(bucketInfo.getObjectID(), Pair.of(volumeId, bucketInfo.getBucketName()));
        bucketVolMap.putIfAbsent(volumeId, Pair.of(null, volumeName));
      }
    }

    // Add dir tree
    try (TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>> directoryIterator = 
        directoryTable.iterator()) {
      
      while (directoryIterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> entry = directoryIterator.next();
        OmDirectoryInfo dirInfo = entry.getValue();
        addToDirTree(dirInfo.getObjectID(), dirInfo.getParentObjectID(), dirInfo.getName());
      }
    }
  }

  private String reconstructFullPath(OmKeyInfo keyInfo, Map<Long, Pair<Long, String>> bucketVolMap,
      Map<Long, Long> unreferencedCountMap, Set<Long> keyContainers) throws Exception {
    StringBuilder sb = new StringBuilder(keyInfo.getKeyName());
    Long prvParent = keyInfo.getParentObjectID();
    
    while (prvParent != null) {
      // Check reached for bucket volume level
      if (bucketVolMap.containsKey(prvParent)) {
        Pair<Long, String> nameParentPair = bucketVolMap.get(prvParent);
        sb.insert(0, nameParentPair.getValue() + OM_KEY_PREFIX);
        prvParent = nameParentPair.getKey();
        if (null == prvParent) {
          return OM_KEY_PREFIX + sb;
        }
        continue;
      }

      // Check dir tree
      Pair<Long, String> nameParentPair = getFromDirTree(prvParent);
      if (nameParentPair == null) {
        // If parent is not found, mark the key as unreferenced and increment its count
        for (Long containerId : keyContainers) {
          unreferencedCountMap.put(containerId, unreferencedCountMap.get(containerId) + 1);
        }
        return "[unreferenced] " + keyInfo.getKeyName();
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
    OmVolumeArgs volumeArgs = volumeTable.get(volumeKey);
    
    if (volumeArgs != null) {
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

  private void jsonOutput(PrintWriter writer, Map<Long, List<String>> containerToKeysMap,
      Map<Long, Long> unreferencedCountMap) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode root = mapper.createObjectNode();
      ObjectNode containersNode = mapper.createObjectNode();

      for (Map.Entry<Long, List<String>> entry : containerToKeysMap.entrySet()) {
        Long containerId = entry.getKey();
        ObjectNode containerNode = mapper.createObjectNode();
        ArrayNode keysArray = mapper.createArrayNode();
        
        for (String key : entry.getValue()) {
          keysArray.add(key);
        }
        
        containerNode.set("keys", keysArray);
        containerNode.put("totalKeys", entry.getValue().size()); // includes unreferenced keys
        
        // Add unreferenced count if > 0
        long unreferencedCount = unreferencedCountMap.get(containerId);
        if (unreferencedCount > 0) {
          containerNode.put("unreferencedKeys", unreferencedCount);
        }
        
        containersNode.set(containerId.toString(), containerNode);
      }

      root.set("containers", containersNode);
      writer.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
      writer.flush();
    } catch (Exception e) {
      err().println("Error writing JSON output: " + e.getMessage());
    }
  }
}


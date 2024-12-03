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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Tool that retrieve key full path from OM db file table with pattern match.
 */
@CommandLine.Command(
    name = "retrieve-key-fullpath",
    description = "retrieve full key path with matching criteria")
@MetaInfServices(SubcommandWithParent.class)
public class KeyPathRetriever implements Callable<Void>, SubcommandWithParent {
  private static final String DIRTREEDBNAME = "omdirtree.db";
  private static final DBColumnFamilyDefinition<Long, String> DIRTREE_COLUMN_FAMILY
      = new DBColumnFamilyDefinition<>("dirTreeTable", LongCodec.get(), StringCodec.get());

  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database File")
  private String dbFile;

  @CommandLine.Option(names = {"--containers"},
      required = false,
      description = "Comma separated Container Ids")
  private String strContainerIds;

  @CommandLine.Option(names = {"--container-file"},
      required = false,
      description = "file with container id in new line")
  private String containerFileName;

  private DBStore dirTreeDbStore = null;
  private Table<Long, String> dirTreeTable = null;

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  public void setDirTreeTable(Table<Long, String> table) {
    this.dirTreeTable = table;
  }

  @Override
  public Void call() throws Exception {
    // get containerIds filter
    Set<Long> containerIds = new HashSet<>();
    if (!StringUtils.isEmpty(strContainerIds)) {
      String[] split = strContainerIds.split(",");
      for (String id : split) {
        containerIds.add(Long.parseLong(id));
      }
    } else if (!StringUtils.isEmpty(containerFileName)) {
      try (Stream<String> stream = Files.lines(Paths.get(containerFileName))) {
        stream.forEach(e -> containerIds.add(Long.parseLong(e)));
      }
    }
    if (containerIds.isEmpty()) {
      System.err.println("No containers to be filtered");
      return null;
    }
    // out stream
    PrintWriter writer = out();

    // db handler
    OMMetadataManager metadataManager = getOmMetadataManager(dbFile);
    if (metadataManager == null) {
      writer.close();
      return null;
    }

    dirTreeDbStore = openDb(new File(dbFile).getParentFile());
    if (dirTreeDbStore == null) {
      writer.close();
      metadataManager.stop();
      return null;
    }
    dirTreeTable = dirTreeDbStore.getTable(DIRTREE_COLUMN_FAMILY.getName(), Long.class, String.class);

    try {
      retrieve(metadataManager, writer, containerIds);
    } finally {
      dirTreeDbStore.close();
      writer.close();
      metadataManager.stop();
      // delete temporary created db file
      File dirTreeDbPath = new File(new File(dbFile).getParentFile(), DIRTREEDBNAME);
      if (dirTreeDbPath.exists()) {
        FileUtils.deleteDirectory(dirTreeDbPath);
      }
    }
    return null;
  }

  public void retrieve(
      OMMetadataManager metadataManager, PrintWriter writer, Set<Long> containerIds) {
    // build dir tree
    Map<Long, Pair<Long, String>> bucketVolMap = new HashMap<>();
    try {
      prepareDirIdTree(metadataManager, bucketVolMap);
    } catch (Exception e) {
      System.err.println("Exception occurred reading directory Table, " + e);
      return;
    }

    // iterate file table and filter for container
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>> fileItr
             = metadataManager.getFileTable().iterator()) {
      while (fileItr.hasNext()) {
        KeyValue<String, OmKeyInfo> next = fileItr.next();
        boolean found = next.getValue().getKeyLocationVersions().stream().anyMatch(
            e -> e.getLocationList().stream().anyMatch(
                blk -> containerIds.contains(blk.getBlockID().getContainerID())));
        if (found) {
          StringBuilder sb = new StringBuilder(next.getValue().getKeyName());
          Long prvParent = next.getValue().getParentObjectID();
          while (prvParent != null) {
            // check reached for bucket volume level
            if (bucketVolMap.containsKey(prvParent)) {
              Pair<Long, String> nameParentPair = bucketVolMap.get(prvParent);
              sb.insert(0, nameParentPair.getValue() + OM_KEY_PREFIX);
              prvParent = nameParentPair.getKey();
              if (null == prvParent) {
                // add to output as reached till volume
                writer.println(sb);
                break;
              }
              continue;
            }

            // check dir tree
            Pair<Long, String> nameParentPair = getFromDirTree(prvParent);
            if (nameParentPair == null) {
              break;
            }
            sb.insert(0, nameParentPair.getValue() + OM_KEY_PREFIX);
            prvParent = nameParentPair.getKey();
          }
        }
      }
    } catch (IOException e) {
      System.err.println("Exception occurred reading file Table, " + e);
    }
  }

  private static OMMetadataManager getOmMetadataManager(String db) throws IOException {
    if (!Files.exists(Paths.get(db))) {
      System.err.println("DB with path not exist:" + db);
      return null;
    }
    System.err.println("Db Path is:" + db);
    File file = new File(db);

    OzoneConfiguration conf = new OzoneConfiguration();
    return new OmMetadataManagerImpl(conf, file.getParentFile(), file.getName());
  }

  private void prepareDirIdTree(
      OMMetadataManager metadataManager, Map<Long, Pair<Long, String>> bucketVolMap) throws IOException {
    // add bucket volume tree
    try (TableIterator<String, ? extends KeyValue<String, OmBucketInfo>> bucItr
             = metadataManager.getBucketTable().iterator()) {
      while (bucItr.hasNext()) {
        KeyValue<String, OmBucketInfo> next = bucItr.next();
        bucketVolMap.put(next.getValue().getObjectID(),
            Pair.of(metadataManager.getVolumeId(next.getValue().getVolumeName()), next.getValue().getBucketName()));
        bucketVolMap.putIfAbsent(metadataManager.getVolumeId(next.getValue().getVolumeName()),
            Pair.of(null, next.getValue().getVolumeName()));
      }
    }
    // add dir tree
    try (TableIterator<String, ? extends KeyValue<String, OmDirectoryInfo>> dirItr
             = metadataManager.getDirectoryTable().iterator()) {
      while (dirItr.hasNext()) {
        KeyValue<String, OmDirectoryInfo> next = dirItr.next();
        addToDirTree(next.getValue().getObjectID(), next.getValue().getParentObjectID(), next.getValue().getName());
      }
    }
  }

  private DBStore openDb(File omPath) {
    File dirTreeDbPath = new File(omPath, DIRTREEDBNAME);
    System.err.println("Creating database of dir tree path at " + dirTreeDbPath);
    try {
      // Delete the DB from the last run if it exists.
      if (dirTreeDbPath.exists()) {
        FileUtils.deleteDirectory(dirTreeDbPath);
      }
      ConfigurationSource conf = new OzoneConfiguration();
      DBStoreBuilder dbStoreBuilder = DBStoreBuilder.newBuilder(conf);
      dbStoreBuilder.setName(dirTreeDbPath.getName());
      dbStoreBuilder.setPath(dirTreeDbPath.getParentFile().toPath());
      dbStoreBuilder.addTable(DIRTREE_COLUMN_FAMILY.getName());
      dbStoreBuilder.addCodec(DIRTREE_COLUMN_FAMILY.getKeyType(), DIRTREE_COLUMN_FAMILY.getKeyCodec());
      dbStoreBuilder.addCodec(DIRTREE_COLUMN_FAMILY.getValueType(), DIRTREE_COLUMN_FAMILY.getValueCodec());
      return dbStoreBuilder.build();
    } catch (IOException e) {
      System.err.println("Error creating omdirtree.db " + e);
      return null;
    }
  }

  private void addToDirTree(Long objectId, Long parentId, String name) throws IOException {
    if (null == parentId) {
      dirTreeTable.put(objectId, name + "#");
    } else {
      dirTreeTable.put(objectId, name + "#" + parentId);
    }
  }

  private Pair<Long, String> getFromDirTree(Long objectId) throws IOException {
    String val = dirTreeTable.get(objectId);
    if (null == val) {
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

  private static PrintWriter out() {
    return spec.commandLine().getOut();
  }
}
